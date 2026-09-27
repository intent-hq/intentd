//! Policy composition using the exported core DTOs and provider errors.

#[expect(dead_code, reason = "this target tests the typed policy subset")]
#[path = "../src/observation_adapter.rs"]
mod observation_adapter;
#[path = "../src/observation_policy.rs"]
mod observation_policy;

use intent_core::{
    ExecutionScope, RepositoryConnectionScope, RepositoryProvider, RepositoryResourceKind,
    RepositoryTarget,
};
use intent_sourcecontrol::{
    error::{ProviderFailure, ProviderFailureKind},
    Error, MergeRequirementSignals, PrState, ProviderAvailability, PullRequest, RateLimitStatus,
    ReviewAvailability, ReviewDetails, ReviewObservation, ReviewThreadTally,
};
use observation_adapter::{ConnectionObservations, ObservationReceipt, ReviewObservations};
use observation_policy::{Coverage, Ineligible};

fn connection(id: &str) -> ConnectionObservations {
    ConnectionObservations::new(
        ExecutionScope {
            daemon_id: "daemon-A".into(),
            authority_scope_id: "caller-workspace-1".into(),
            authority_generation: 9_007_199_254_740_995,
        },
        RepositoryConnectionScope {
            connection_id: id.into(),
            account_id: format!("account-{id}"),
            connection_generation: u64::MAX,
        },
    )
}

fn repository(path: &str) -> RepositoryTarget {
    RepositoryTarget {
        provider: RepositoryProvider::Gitlab,
        instance_base_url: "https://git.example:8443/forge".into(),
        project_path: path.into(),
    }
}

fn denied(kind: ProviderFailureKind) -> Error {
    // Classification comes from the actual provider enum, never a status rule here.
    Error::Provider(ProviderFailure { kind, status: None })
}

fn warm(slot: &mut ReviewObservations, coverage: Coverage) -> ObservationReceipt {
    let read = slot.begin(coverage).unwrap();
    slot.primary_success(read).unwrap()
}

fn warm_both(slot: &mut ReviewObservations) -> [ObservationReceipt; 2] {
    [warm(slot, Coverage::Detail), warm(slot, Coverage::Summary)]
}

#[test]
fn credential_rejection_invalidates_the_connection_but_not_another_connection() {
    let connection = connection("gitlab");
    let project = connection.project(repository("team/app"));
    let other_project = connection.project(repository("team/other"));
    let mut affected = [
        project.slot(RepositoryResourceKind::MergeRequest, 7),
        project.slot(RepositoryResourceKind::Issue, 7),
        other_project.slot(RepositoryResourceKind::MergeRequest, 8),
    ];
    let warm: Vec<_> = affected.iter_mut().map(warm_both).collect();
    let old: Vec<_> = affected
        .iter_mut()
        .map(|slot| slot.begin(Coverage::Detail).unwrap())
        .collect();
    let old_summaries: Vec<_> = affected
        .iter_mut()
        .map(|slot| slot.begin(Coverage::Summary).unwrap())
        .collect();
    let unrelated = self::connection("github");
    let mut other = unrelated
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let other_warm = warm_both(&mut other);
    let other_read = other.begin(Coverage::Detail).unwrap();

    let read = affected[0].begin(Coverage::Detail).unwrap();
    let (error, _) = affected[0]
        .failure(
            read,
            denied(ProviderFailureKind::CredentialRejected),
            RateLimitStatus::default(),
        )
        .unwrap();
    assert!(matches!(
        error,
        Error::Provider(ProviderFailure {
            kind: ProviderFailureKind::CredentialRejected,
            ..
        })
    ));
    for (slot, receipts) in affected.iter().zip(&warm) {
        for receipt in receipts {
            assert!(!slot.can_serve(receipt, &connection));
        }
    }
    for (slot, read) in affected.iter_mut().zip(old) {
        assert_eq!(
            slot.primary_success(read).unwrap_err(),
            Ineligible::DeniedSinceRequest
        );
    }
    let recovered = self::warm(&mut affected[0], Coverage::Summary);
    assert!(affected[0].can_serve(&recovered, &connection));
    for (slot, read) in affected.iter_mut().zip(old_summaries) {
        assert_eq!(
            slot.primary_success(read).unwrap_err(),
            Ineligible::DeniedSinceRequest
        );
    }
    assert!(affected[0].can_serve(&recovered, &connection));
    for (slot, receipts) in affected.iter().zip(&warm) {
        for receipt in receipts {
            assert!(!slot.can_serve(receipt, &connection));
        }
    }
    assert!(other_warm
        .iter()
        .all(|receipt| other.can_serve(receipt, &unrelated)));
    assert!(other.primary_success(other_read).is_ok());
}

#[test]
fn project_denial_covers_mrs_and_issues_but_preserves_other_projects_and_connections() {
    let connection = connection("gitlab");
    let project = connection.project(repository("team/app"));
    // Independent handles for the same canonical project must share invalidation.
    let same_project = connection.clone().project(repository("team/app"));
    let mut affected = [
        project.slot(RepositoryResourceKind::MergeRequest, 7),
        same_project.slot(RepositoryResourceKind::Issue, 8),
    ];
    let warm: Vec<_> = affected.iter_mut().map(warm_both).collect();
    let old: Vec<_> = affected
        .iter_mut()
        .map(|slot| {
            (
                slot.begin(Coverage::Detail).unwrap(),
                slot.begin(Coverage::Summary).unwrap(),
            )
        })
        .collect();
    let mut elsewhere = connection
        .project(repository("team/other"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let elsewhere_warm = warm_both(&mut elsewhere);
    let unrelated = self::connection("another-account");
    let mut other = unrelated
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::Issue, 8);
    let other_warm = warm_both(&mut other);
    let read = affected[0].begin(Coverage::Detail).unwrap();
    affected[0]
        .failure(
            read,
            denied(ProviderFailureKind::ProjectDenied),
            RateLimitStatus::default(),
        )
        .unwrap();
    for (slot, receipts) in affected.iter().zip(&warm) {
        assert!(receipts
            .iter()
            .all(|receipt| !slot.can_serve(receipt, &connection)));
    }
    let recovered = self::warm(&mut affected[0], Coverage::Summary);
    assert!(affected[0].can_serve(&recovered, &connection));
    for (slot, (detail, summary)) in affected.iter_mut().zip(old) {
        assert_eq!(
            slot.primary_success(detail).unwrap_err(),
            Ineligible::DeniedSinceRequest
        );
        // On the recovered item the sequence fence can reject this first;
        // on its sibling the unchanged project-denial fence rejects it.
        assert!(matches!(
            slot.primary_success(summary),
            Err(Ineligible::DeniedSinceRequest | Ineligible::OlderObservation)
        ));
    }
    assert!(affected[0].can_serve(&recovered, &connection));
    assert!(!affected[0].can_serve(&warm[0][0], &connection));
    assert!(warm[1]
        .iter()
        .all(|receipt| !affected[1].can_serve(receipt, &connection)));
    assert!(elsewhere_warm
        .iter()
        .all(|receipt| elsewhere.can_serve(receipt, &connection)));
    assert!(other_warm
        .iter()
        .all(|receipt| other.can_serve(receipt, &unrelated)));
}

#[test]
fn resource_denial_is_not_a_project_denial() {
    let connection = connection("gitlab");
    let project = connection.project(repository("team/app"));
    let mut item = project.slot(RepositoryResourceKind::MergeRequest, 7);
    let mut sibling = project.slot(RepositoryResourceKind::MergeRequest, 8);
    let mut issue = project.slot(RepositoryResourceKind::Issue, 7);
    let previous = warm_both(&mut item);
    let sibling_warm = warm_both(&mut sibling);
    let issue_warm = warm_both(&mut issue);
    let old = item.begin(Coverage::Summary).unwrap();
    let read = item.begin(Coverage::Detail).unwrap();
    item.failure(
        read,
        denied(ProviderFailureKind::ResourceDenied),
        RateLimitStatus::default(),
    )
    .unwrap();
    assert!(previous
        .iter()
        .all(|receipt| !item.can_serve(receipt, &connection)));
    let recovered = warm(&mut item, Coverage::Detail);
    assert!(item.can_serve(&recovered, &connection));
    assert_eq!(
        item.primary_success(old).unwrap_err(),
        Ineligible::DeniedSinceRequest
    );
    assert!(!item.can_serve(&previous[1], &connection));
    assert!(sibling_warm
        .iter()
        .all(|receipt| sibling.can_serve(receipt, &connection)));
    assert!(issue_warm
        .iter()
        .all(|receipt| issue.can_serve(receipt, &connection)));
}

#[test]
fn a_wrong_item_ticket_cannot_apply_a_connection_wide_denial() {
    let connection = connection("gitlab");
    let project = connection.project(repository("team/app"));
    let mut item = project.slot(RepositoryResourceKind::MergeRequest, 7);
    let mut other = project.slot(RepositoryResourceKind::Issue, 7);
    let retained = warm_both(&mut item);
    let other_warm = warm_both(&mut other);
    let ticket = item.begin(Coverage::Detail).unwrap();
    assert_eq!(
        other
            .failure(
                ticket,
                denied(ProviderFailureKind::CredentialRejected),
                RateLimitStatus::default()
            )
            .unwrap_err(),
        Ineligible::DifferentSlot
    );
    assert!(retained
        .iter()
        .all(|receipt| item.can_serve(receipt, &connection)));
    assert!(other_warm
        .iter()
        .all(|receipt| other.can_serve(receipt, &connection)));
}

#[test]
fn retired_connection_results_cannot_invalidate_a_replacement() {
    let old = connection("gitlab");
    let mut item = old
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let retained = warm(&mut item, Coverage::Detail);
    let success = item.begin(Coverage::Detail).unwrap();
    let denial = item.begin(Coverage::Detail).unwrap();
    let replacement = connection("gitlab");
    let mut current = replacement
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let current_warm = warm(&mut current, Coverage::Detail);
    assert!(!item.can_serve(&retained, &replacement));
    old.retire();
    assert_eq!(
        item.primary_success(success).unwrap_err(),
        Ineligible::RetiredScope
    );
    assert_eq!(
        item.failure(
            denial,
            denied(ProviderFailureKind::CredentialRejected),
            RateLimitStatus::default()
        )
        .unwrap_err(),
        Ineligible::RetiredScope
    );
    assert!(current.can_serve(&current_warm, &replacement));
}

#[test]
fn canonical_types_and_precise_generations_form_the_actual_key() {
    let connection = connection("gitlab");
    let item = connection
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let key = item.key();
    assert_eq!(
        serde_json::to_value(&key.scope.0).unwrap()["authorityGeneration"],
        "9007199254740995"
    );
    assert_eq!(
        serde_json::to_value(&key.scope.1).unwrap()["connectionGeneration"],
        "18446744073709551615"
    );
    assert_eq!(key.resource.repository, repository("team/app"));
    assert_eq!(key.resource.kind, RepositoryResourceKind::MergeRequest);
    assert_eq!(key.resource.number, 7);
    let mut execution = key.scope.0.clone();
    execution.authority_generation -= 1;
    let replacement = ConnectionObservations::new(execution, key.scope.1.clone());
    let changed = replacement
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    assert_ne!(key, changed.key());
}

#[test]
fn optional_transient_unknown_and_quota_errors_are_preserved_without_refresh_or_recovery() {
    let connection = connection("gitlab");
    let mut item = connection
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let retained = warm_both(&mut item);
    for has_denial in [false, true] {
        if has_denial {
            let read = item.begin(Coverage::Detail).unwrap();
            item.failure(
                read,
                denied(ProviderFailureKind::ResourceDenied),
                RateLimitStatus::default(),
            )
            .unwrap();
        }
        for error in [
            denied(ProviderFailureKind::OptionalRestricted),
            denied(ProviderFailureKind::OptionalUnavailable),
            denied(ProviderFailureKind::Transient),
            denied(ProviderFailureKind::Unknown),
            denied(ProviderFailureKind::WriteUncertain),
            Error::RateLimited("quota evidence".into()),
            Error::Auth("original admission error".into()),
            Error::NotConfigured("original missing binding".into()),
        ] {
            let expected = format!("{error:?}");
            let quota = RateLimitStatus {
                remaining: Some(0),
                reset_at: Some(2_000_000_000),
                limit: Some(100),
            };
            let read = item.begin(Coverage::Detail).unwrap();
            let (returned, retained_quota) = item.failure(read, error, quota).unwrap();
            assert_eq!(format!("{returned:?}"), expected);
            assert_eq!(retained_quota, quota);
            for receipt in &retained {
                assert_eq!(item.can_serve(receipt, &connection), !has_denial);
            }
        }
    }
}

fn complete_observation() -> ReviewObservation {
    ReviewObservation {
        details: ReviewDetails {
            review: PullRequest {
                number: 7,
                url: "https://git.example:8443/forge/team/app/-/merge_requests/7".into(),
                title: "Review".into(),
                body: None,
                state: PrState::Open,
                draft: false,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                author: "alice".into(),
                mergeable: None,
                mergeable_state: None,
                head_sha: Some("head-A".into()),
                created_at: String::new(),
                updated_at: String::new(),
            },
            source: None,
            target: None,
            confirmed_draft: None,
            confirmed_state: None,
        },
        signals: MergeRequirementSignals {
            checks_known: true,
            ..Default::default()
        },
        reviews: Some(vec![]),
        threads: Some(ReviewThreadTally::default()),
        conversation_count: Some(0),
        availability: ReviewAvailability {
            policy: ProviderAvailability::Available,
            approvals: ProviderAvailability::Available,
            checks: ProviderAvailability::Available,
            discussions: ProviderAvailability::Available,
        },
    }
}

#[test]
fn partial_observations_preserve_fields_and_backoff_without_a_complete_receipt() {
    let connection = connection("gitlab");
    let mut item = connection
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    let warm = warm_both(&mut item);
    for availability in [
        ProviderAvailability::Restricted,
        ProviderAvailability::Unavailable,
        ProviderAvailability::Transient,
        ProviderAvailability::RateLimited,
        ProviderAvailability::Unknown,
    ] {
        let mut observation = complete_observation();
        observation.availability.approvals = availability;
        observation.reviews = None;
        let expected = observation.clone();
        let quota = RateLimitStatus {
            remaining: Some(0),
            reset_at: Some(2_000_000_000),
            limit: None,
        };
        let read = item.begin(Coverage::Summary).unwrap();
        let result = item.snapshot(read, observation, quota).unwrap();
        assert_eq!(result.observation, expected);
        assert_eq!(result.quota, quota);
        assert!(result.receipt.is_none());
        assert!(warm
            .iter()
            .all(|receipt| item.can_serve(receipt, &connection)));
    }
}

#[test]
fn nullable_or_unknown_fields_do_not_become_complete_snapshots() {
    let connection = connection("gitlab");
    let mut item = connection
        .project(repository("team/app"))
        .slot(RepositoryResourceKind::MergeRequest, 7);
    for field in ["reviews", "threads", "count", "checks"] {
        let mut observation = complete_observation();
        match field {
            "reviews" => observation.reviews = None,
            "threads" => observation.threads = None,
            "count" => observation.conversation_count = None,
            "checks" => observation.signals.checks_known = false,
            _ => unreachable!(),
        }
        let read = item.begin(Coverage::Detail).unwrap();
        assert!(item
            .snapshot(read, observation, RateLimitStatus::default())
            .unwrap()
            .receipt
            .is_none());
    }
}

#[test]
fn a_fresh_complete_snapshot_recovers_only_its_item_and_coverage() {
    let connection = connection("gitlab");
    let project = connection.project(repository("team/app"));
    let mut item = project.slot(RepositoryResourceKind::MergeRequest, 7);
    let mut sibling = project.slot(RepositoryResourceKind::Issue, 8);
    let previous = warm_both(&mut item);
    let sibling_warm = warm_both(&mut sibling);
    let old = item.begin(Coverage::Detail).unwrap();
    let poll = item.begin(Coverage::Detail).unwrap();
    item.failure(
        poll,
        denied(ProviderFailureKind::ProjectDenied),
        RateLimitStatus::default(),
    )
    .unwrap();
    let mut partial = complete_observation();
    partial.availability.approvals = ProviderAvailability::Restricted;
    partial.reviews = None;
    let fresh = item.begin(Coverage::Summary).unwrap();
    assert!(item
        .snapshot(fresh, partial, RateLimitStatus::default())
        .unwrap()
        .receipt
        .is_none());
    assert!(previous
        .iter()
        .all(|receipt| !item.can_serve(receipt, &connection)));
    let fresh = item.begin(Coverage::Summary).unwrap();
    let result = item
        .snapshot(fresh, complete_observation(), RateLimitStatus::default())
        .unwrap();
    assert!(item.can_serve(&result.receipt.unwrap(), &connection));
    assert!(!item.can_serve(&previous[0], &connection));
    assert!(sibling_warm
        .iter()
        .all(|receipt| !sibling.can_serve(receipt, &connection)));
    assert_eq!(
        item.snapshot(old, complete_observation(), RateLimitStatus::default())
            .unwrap_err(),
        Ineligible::DeniedSinceRequest
    );
}
