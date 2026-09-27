//! Qualified repository context and pure review-target selection.
//!
//! Providers supply canonical identities and sanitized transport facts. This
//! module performs no URL parsing, Git/config inspection, provenance recovery,
//! persistence or authorization. In particular, deserializing these DTOs does
//! not establish that their authority or historical evidence is valid.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{WorkspaceGitRootId, WorkspaceId};

/// Forge implementation; namespace spelling never chooses a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepositoryProvider {
    Github,
    Gitlab,
}

/// Provider-canonical project identity, independent of credentials and remotes.
///
/// `instance_base_url` is the logical instance root, including effective port
/// and installation prefix, not a test/transport override. Project path casing
/// and canonicalization belong to the provider; core never folds or truncates
/// them. A provider project ID is corroboration, not another identity key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryTarget {
    pub provider: RepositoryProvider,
    pub instance_base_url: String,
    pub project_path: String,
}

/// Project-local resource numbers are distinct across kinds as well as forges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryResourceKind {
    PullRequest,
    MergeRequest,
    Issue,
}

/// Qualified identity shared by review and adjacent issue observations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewTarget {
    pub repository: RepositoryTarget,
    pub kind: RepositoryResourceKind,
    pub number: u64,
}

/// Trusted service projection of the admitted daemon/caller/workspace view.
///
/// The authority scope identifies the caller and its workspace policy. It is
/// not an authorization parameter: services must derive it from trusted caller
/// context. A mixed-forge context has this common scope and separate connection
/// scopes on its targets.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionScope {
    pub daemon_id: String,
    pub authority_scope_id: String,
    pub authority_generation: u64,
}

/// Non-secret repository account scope; combine with `ExecutionScope` for keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryConnectionScope {
    pub connection_id: String,
    pub account_id: String,
    pub connection_generation: u64,
}

/// Opaque revision token. No global ordering exists across scopes or epochs.
///
/// Producers advance the sequence on observed context changes and replace the
/// epoch at restart. Consumers must use `compare_in_scopes`, not order serialized
/// tokens or treat revision freshness as authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextRevision {
    epoch: String,
    sequence: u64,
}

impl RepositoryContextRevision {
    #[must_use]
    pub fn new(epoch: impl Into<String>, sequence: u64) -> Self {
        Self {
            epoch: epoch.into(),
            sequence,
        }
    }

    /// `None` means replacement/incomparable, never permission to reuse a token.
    #[must_use]
    pub fn compare_in_scopes(
        &self,
        scope: &ExecutionScope,
        other: &Self,
        other_scope: &ExecutionScope,
    ) -> Option<Ordering> {
        (scope == other_scope && self.epoch == other.epoch)
            .then(|| self.sequence.cmp(&other.sequence))
    }
}

/// Root identities are workspace-bound; paths are not interchangeable IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryRootId {
    pub workspace_id: WorkspaceId,
    #[serde(flatten)]
    pub kind: RepositoryRootKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum RepositoryRootKind {
    Primary,
    Registered { git_root_id: WorkspaceGitRootId },
}

/// A remote that cannot be mapped is retained, never dropped to create certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryUnresolvedReason {
    UnknownInstance,
    UnsupportedTransport,
    AmbiguousMapping,
    InvalidRemote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum RepositoryEndpointResolution {
    Resolved { target: RepositoryTarget },
    Unresolved { reason: RepositoryUnresolvedReason },
}

/// The inventory producer must sanitize `url` before constructing this DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryRemoteEndpoint {
    pub url: String,
    pub resolution: RepositoryEndpointResolution,
}

/// Effective transport destinations. Only `fetch` participates in selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryRemote {
    pub name: String,
    pub fetch: Vec<RepositoryRemoteEndpoint>,
    pub push: Vec<RepositoryRemoteEndpoint>,
}

/// Stored provenance is supplied by a separately verified migration, not inferred
/// from current remotes. Monitor overrides are not historical default evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HistoricalTargetSource {
    WorkspaceMetadata,
    RegisteredRootMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalTargetProvenance {
    pub source: HistoricalTargetSource,
    pub record_id: String,
    pub resolver_version: String,
    pub evidence_id: String,
}

/// Persistent Intent choice, independent of Git configuration and per-call input.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum SavedReviewSelection {
    #[default]
    Automatic,
    ExplicitRemote {
        remote_name: String,
    },
    MigratedCanonical {
        target: RepositoryTarget,
        provenance: HistoricalTargetProvenance,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewSelectionSource {
    Automatic,
    ExplicitCall,
    ExplicitRemote,
    MigratedCanonical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewSelectionRequiredReason {
    AmbiguousTargets,
    UnresolvedCandidates,
    MissingSelectedRemote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryUnavailableReason {
    NoRemote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum ReviewSelectionOutcome {
    Resolved {
        target: RepositoryTarget,
        source: ReviewSelectionSource,
    },
    SelectionRequired {
        reason: ReviewSelectionRequiredReason,
    },
    RepositoryUnavailable {
        reason: RepositoryUnavailableReason,
        selection_required: bool,
    },
}

/// Retains the saved record even when no implicit hosted target is available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewSelectionResolution {
    pub saved: SavedReviewSelection,
    pub no_remotes: bool,
    pub outcome: ReviewSelectionOutcome,
}

/// Resolve injected canonical fetch facts without changing the stored choice.
///
/// This selects an identity, not a credential or permission. Explicit qualified
/// calls can address a project with no remotes; implicit calls cannot. Neither
/// `origin`, push destinations nor current remotes recover historical intent.
#[must_use]
pub fn resolve_review_selection(
    saved: &SavedReviewSelection,
    remotes: &[RepositoryRemote],
    explicit_target: Option<&RepositoryTarget>,
) -> ReviewSelectionResolution {
    let outcome = if let Some(target) = explicit_target {
        ReviewSelectionOutcome::Resolved {
            target: target.clone(),
            source: ReviewSelectionSource::ExplicitCall,
        }
    } else if remotes.is_empty() {
        ReviewSelectionOutcome::RepositoryUnavailable {
            reason: RepositoryUnavailableReason::NoRemote,
            selection_required: !matches!(saved, SavedReviewSelection::Automatic),
        }
    } else {
        match saved {
            SavedReviewSelection::Automatic => {
                resolve_fetch_candidates(remotes.iter(), ReviewSelectionSource::Automatic)
            }
            SavedReviewSelection::ExplicitRemote { remote_name } => {
                if remotes.iter().any(|remote| remote.name == *remote_name) {
                    resolve_fetch_candidates(
                        remotes.iter().filter(|remote| remote.name == *remote_name),
                        ReviewSelectionSource::ExplicitRemote,
                    )
                } else {
                    ReviewSelectionOutcome::SelectionRequired {
                        reason: ReviewSelectionRequiredReason::MissingSelectedRemote,
                    }
                }
            }
            SavedReviewSelection::MigratedCanonical { target, .. } => {
                ReviewSelectionOutcome::Resolved {
                    target: target.clone(),
                    source: ReviewSelectionSource::MigratedCanonical,
                }
            }
        }
    };
    ReviewSelectionResolution {
        saved: saved.clone(),
        no_remotes: remotes.is_empty(),
        outcome,
    }
}

fn resolve_fetch_candidates<'a>(
    remotes: impl Iterator<Item = &'a RepositoryRemote>,
    source: ReviewSelectionSource,
) -> ReviewSelectionOutcome {
    let mut targets = BTreeSet::new();
    let mut unresolved = false;
    for remote in remotes {
        unresolved |= remote.fetch.is_empty();
        for endpoint in &remote.fetch {
            match &endpoint.resolution {
                RepositoryEndpointResolution::Resolved { target } => {
                    targets.insert(target.clone());
                }
                RepositoryEndpointResolution::Unresolved { .. } => unresolved = true,
            }
        }
    }
    if unresolved {
        return ReviewSelectionOutcome::SelectionRequired {
            reason: ReviewSelectionRequiredReason::UnresolvedCandidates,
        };
    }
    if targets.len() == 1 {
        return ReviewSelectionOutcome::Resolved {
            target: targets.pop_first().expect("one canonical target"),
            source,
        };
    }
    ReviewSelectionOutcome::SelectionRequired {
        reason: ReviewSelectionRequiredReason::AmbiguousTargets,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryAvailability {
    Connected,
    Disconnected,
    Disabled,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryOperation {
    ReadReview,
    ReadIssue,
    CreateReview,
    Clone,
    Fetch,
    Push,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryCapabilityState {
    Available,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCapability {
    pub operation: RepositoryOperation,
    pub state: RepositoryCapabilityState,
}

/// Known target observations, distinct from canonical identity and selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryTargetContext {
    pub target: RepositoryTarget,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<RepositoryConnectionScope>,
    pub availability: RepositoryAvailability,
    pub capabilities: Vec<RepositoryCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryRootContext {
    pub root: RepositoryRootId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    pub remotes: Vec<RepositoryRemote>,
    pub targets: Vec<RepositoryTargetContext>,
    pub review_selection: ReviewSelectionResolution,
}

/// Internal shared DTO; defining it does not register a wire or MCP method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryContext {
    pub revision: RepositoryContextRevision,
    pub scope: ExecutionScope,
    pub roots: Vec<RepositoryRootContext>,
}

impl RepositoryContext {
    #[must_use]
    pub fn compare_revision(&self, other: &Self) -> Option<Ordering> {
        self.revision
            .compare_in_scopes(&self.scope, &other.revision, &other.scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn github(path: &str) -> RepositoryTarget {
        RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: path.into(),
        }
    }

    fn gitlab() -> RepositoryTarget {
        RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: "https://git.example:8443/gitlab".into(),
            project_path: "team/sub/app".into(),
        }
    }

    fn endpoint(target: &RepositoryTarget) -> RepositoryRemoteEndpoint {
        RepositoryRemoteEndpoint {
            url: format!("{}/{}.git", target.instance_base_url, target.project_path),
            resolution: RepositoryEndpointResolution::Resolved {
                target: target.clone(),
            },
        }
    }

    fn remote(name: &str, target: &RepositoryTarget) -> RepositoryRemote {
        RepositoryRemote {
            name: name.into(),
            fetch: vec![endpoint(target)],
            push: vec![],
        }
    }

    fn selected(name: &str) -> SavedReviewSelection {
        SavedReviewSelection::ExplicitRemote {
            remote_name: name.into(),
        }
    }

    fn migrated(target: &RepositoryTarget) -> SavedReviewSelection {
        SavedReviewSelection::MigratedCanonical {
            target: target.clone(),
            provenance: HistoricalTargetProvenance {
                source: HistoricalTargetSource::WorkspaceMetadata,
                record_id: "workspace-1".into(),
                resolver_version: "0.9.112".into(),
                evidence_id: "verified-legacy-target-1".into(),
            },
        }
    }

    fn assert_target(
        result: &ReviewSelectionResolution,
        target: &RepositoryTarget,
        source: ReviewSelectionSource,
    ) {
        assert_eq!(
            result.outcome,
            ReviewSelectionOutcome::Resolved {
                target: target.clone(),
                source,
            }
        );
    }

    fn assert_required(result: &ReviewSelectionResolution, reason: ReviewSelectionRequiredReason) {
        assert_eq!(
            result.outcome,
            ReviewSelectionOutcome::SelectionRequired { reason }
        );
    }

    fn scope() -> ExecutionScope {
        ExecutionScope {
            daemon_id: "daemon-A".into(),
            authority_scope_id: "caller-workspace-1".into(),
            authority_generation: 7,
        }
    }

    #[test]
    fn qualified_identity_keeps_instance_prefix_port_path_and_kind() {
        let base = gitlab();
        let mut targets = BTreeSet::from([base.clone()]);
        for instance in [
            "https://git.example:9443/gitlab",
            "https://git.example:8443/other",
        ] {
            let mut target = base.clone();
            target.instance_base_url = instance.into();
            assert!(targets.insert(target));
        }
        for path in ["other/sub/app", "team/app", "Team/sub/app"] {
            let mut target = base.clone();
            target.project_path = path.into();
            assert!(
                targets.insert(target),
                "core must not truncate or case-fold a path"
            );
        }
        let mut other_provider = base;
        other_provider.provider = RepositoryProvider::Github;
        assert!(targets.insert(other_provider));
        let mut resources = BTreeSet::new();
        for repository in targets {
            for kind in [
                RepositoryResourceKind::PullRequest,
                RepositoryResourceKind::MergeRequest,
                RepositoryResourceKind::Issue,
            ] {
                assert!(resources.insert(ReviewTarget {
                    repository: repository.clone(),
                    kind,
                    number: 42
                }));
            }
        }
        assert_eq!(resources.len(), 21);
    }

    #[test]
    fn automatic_collapses_fetch_aliases_independently_of_transport_spelling() {
        let target = gitlab();
        let mut ssh = remote("mirror", &target);
        ssh.fetch[0].url = "ssh://git@git.example:2222/team/sub/app.git".into();
        let remotes = [remote("origin", &target), ssh];
        let result = resolve_review_selection(&SavedReviewSelection::Automatic, &remotes, None);
        assert_target(&result, &target, ReviewSelectionSource::Automatic);
        let reversed = [remotes[1].clone(), remotes[0].clone()];
        assert_eq!(
            result,
            resolve_review_selection(&SavedReviewSelection::Automatic, &reversed, None)
        );
    }

    #[test]
    fn origin_does_not_win_over_another_project_or_forge() {
        for other in [github("team/other"), gitlab()] {
            let remotes = [
                remote("origin", &github("team/app")),
                remote("upstream", &other),
            ];
            assert_required(
                &resolve_review_selection(&SavedReviewSelection::Automatic, &remotes, None),
                ReviewSelectionRequiredReason::AmbiguousTargets,
            );
        }
    }

    #[test]
    fn unresolved_or_empty_fetch_inventory_cannot_manufacture_unique_selection() {
        for fetch in [
            vec![RepositoryRemoteEndpoint {
                url: "ssh://unknown.example/team/app.git".into(),
                resolution: RepositoryEndpointResolution::Unresolved {
                    reason: RepositoryUnresolvedReason::UnknownInstance,
                },
            }],
            vec![],
        ] {
            let remotes = [
                remote("origin", &github("team/app")),
                RepositoryRemote {
                    name: "other".into(),
                    fetch,
                    push: vec![],
                },
            ];
            assert_required(
                &resolve_review_selection(&SavedReviewSelection::Automatic, &remotes, None),
                ReviewSelectionRequiredReason::UnresolvedCandidates,
            );
        }
    }

    #[test]
    fn multiple_fetch_targets_on_one_selected_remote_are_ambiguous() {
        let mut origin = remote("origin", &github("team/app"));
        origin.fetch.push(endpoint(&gitlab()));
        for choice in [SavedReviewSelection::Automatic, selected("origin")] {
            assert_required(
                &resolve_review_selection(&choice, &[origin.clone()], None),
                ReviewSelectionRequiredReason::AmbiguousTargets,
            );
        }
    }

    #[test]
    fn saved_remote_ignores_unrelated_remotes_and_their_removal() {
        let target = gitlab();
        let choice = selected("review");
        let review = remote("review", &target);
        let unrelated = RepositoryRemote {
            name: "unknown".into(),
            fetch: vec![],
            push: vec![],
        };
        let result = resolve_review_selection(&choice, &[review.clone(), unrelated], None);
        assert_target(&result, &target, ReviewSelectionSource::ExplicitRemote);
        assert_eq!(result, resolve_review_selection(&choice, &[review], None));
    }

    #[test]
    fn saved_remote_follows_changed_fetch_target_without_changing_saved_choice() {
        let choice = selected("origin");
        for target in [github("team/app"), gitlab(), github("team/next")] {
            let result = resolve_review_selection(&choice, &[remote("origin", &target)], None);
            assert_target(&result, &target, ReviewSelectionSource::ExplicitRemote);
            assert_eq!(result.saved, choice);
        }
    }

    #[test]
    fn fetch_push_mismatch_in_both_directions_never_changes_the_default() {
        let gh = github("team/app");
        let gl = gitlab();
        for (fetch, push) in [(&gh, &gl), (&gl, &gh)] {
            let mut origin = remote("origin", fetch);
            origin.push = vec![endpoint(push)];
            for (choice, source) in [
                (
                    SavedReviewSelection::Automatic,
                    ReviewSelectionSource::Automatic,
                ),
                (selected("origin"), ReviewSelectionSource::ExplicitRemote),
            ] {
                let result = resolve_review_selection(&choice, &[origin.clone()], None);
                assert_target(&result, fetch, source);
                let explicit = resolve_review_selection(&choice, &[origin.clone()], Some(push));
                assert_target(&explicit, push, ReviewSelectionSource::ExplicitCall);
                assert_eq!(explicit.saved, choice);
                assert_eq!(
                    result,
                    resolve_review_selection(&choice, &[origin.clone()], None)
                );
            }
        }
    }

    #[test]
    fn push_only_target_is_not_an_automatic_candidate() {
        let origin = RepositoryRemote {
            name: "origin".into(),
            fetch: vec![],
            push: vec![endpoint(&gitlab())],
        };
        assert_required(
            &resolve_review_selection(&SavedReviewSelection::Automatic, &[origin], None),
            ReviewSelectionRequiredReason::UnresolvedCandidates,
        );
    }

    #[test]
    fn missing_or_renamed_selection_survives_serialization_without_fallback() {
        let choice = selected("review");
        let persisted = serde_json::to_value(&choice).unwrap();
        for remotes in [
            vec![remote("origin", &github("team/app"))],
            vec![remote("renamed", &gitlab())],
        ] {
            let restored = serde_json::from_value(persisted.clone()).unwrap();
            let result = resolve_review_selection(&restored, &remotes, None);
            assert_eq!(result.saved, choice);
            assert_required(
                &result,
                ReviewSelectionRequiredReason::MissingSelectedRemote,
            );
        }
        assert_target(
            &resolve_review_selection(&selected("renamed"), &[remote("renamed", &gitlab())], None),
            &gitlab(),
            ReviewSelectionSource::ExplicitRemote,
        );
    }

    #[test]
    fn zero_remotes_reports_no_default_and_retains_every_saved_state() {
        for choice in [
            SavedReviewSelection::Automatic,
            selected("review"),
            migrated(&github("team/old")),
        ] {
            let result = resolve_review_selection(&choice, &[], None);
            assert!(result.no_remotes);
            assert_eq!(result.saved, choice);
            assert_eq!(
                result.outcome,
                ReviewSelectionOutcome::RepositoryUnavailable {
                    reason: RepositoryUnavailableReason::NoRemote,
                    selection_required: !matches!(choice, SavedReviewSelection::Automatic),
                }
            );
            let restored: ReviewSelectionResolution =
                serde_json::from_value(serde_json::to_value(&result).unwrap()).unwrap();
            assert_eq!(restored, result);
        }
    }

    #[test]
    fn explicit_cross_repo_call_does_not_persist_even_without_remotes() {
        let choice = selected("missing");
        let before = resolve_review_selection(&choice, &[], None);
        let explicit = resolve_review_selection(&choice, &[], Some(&gitlab()));
        assert!(explicit.no_remotes);
        assert_target(&explicit, &gitlab(), ReviewSelectionSource::ExplicitCall);
        assert_eq!(explicit.saved, choice);
        assert_eq!(before, resolve_review_selection(&choice, &[], None));
    }

    #[test]
    fn clearing_a_saved_choice_restores_strict_automatic_discovery() {
        let remotes = [remote("origin", &gitlab())];
        assert_required(
            &resolve_review_selection(&selected("missing"), &remotes, None),
            ReviewSelectionRequiredReason::MissingSelectedRemote,
        );
        assert_target(
            &resolve_review_selection(&SavedReviewSelection::Automatic, &remotes, None),
            &gitlab(),
            ReviewSelectionSource::Automatic,
        );
        let mixed = [remotes[0].clone(), remote("upstream", &github("team/app"))];
        assert_required(
            &resolve_review_selection(&SavedReviewSelection::Automatic, &mixed, None),
            ReviewSelectionRequiredReason::AmbiguousTargets,
        );
    }

    #[test]
    fn migrated_target_survives_changed_origin_and_restart_without_rebinding() {
        let previous = github("team/previous");
        let saved = migrated(&previous);
        let restored = serde_json::from_value(serde_json::to_value(&saved).unwrap()).unwrap();
        let remotes = [
            remote("origin", &gitlab()),
            remote("upstream", &github("team/new")),
        ];
        let result = resolve_review_selection(&restored, &remotes, None);
        assert_target(&result, &previous, ReviewSelectionSource::MigratedCanonical);
        assert_eq!(result.saved, saved);
        assert_target(
            &resolve_review_selection(&restored, &remotes, Some(&gitlab())),
            &gitlab(),
            ReviewSelectionSource::ExplicitCall,
        );
        assert_eq!(result, resolve_review_selection(&restored, &remotes, None));
    }

    #[test]
    fn current_origin_does_not_create_migration_provenance() {
        let remotes = [
            remote("origin", &gitlab()),
            remote("upstream", &github("team/old")),
        ];
        let result = resolve_review_selection(&SavedReviewSelection::Automatic, &remotes, None);
        assert_eq!(result.saved, SavedReviewSelection::Automatic);
        assert_required(&result, ReviewSelectionRequiredReason::AmbiguousTargets);
        assert!(serde_json::from_value::<SavedReviewSelection>(
            json!({"mode":"migrated-canonical","target":github("team/old")})
        )
        .is_err());
    }

    #[test]
    fn revision_ordering_is_numeric_only_inside_the_same_scope_and_epoch() {
        let old = RepositoryContextRevision::new("boot", 2);
        let new = RepositoryContextRevision::new("boot", 12);
        assert_eq!(
            old.compare_in_scopes(&scope(), &new, &scope()),
            Some(Ordering::Less)
        );
        assert_eq!(
            new.compare_in_scopes(&scope(), &old, &scope()),
            Some(Ordering::Greater)
        );
        assert_eq!(
            new.compare_in_scopes(&scope(), &new, &scope()),
            Some(Ordering::Equal)
        );
        for (epoch, sequence) in [("replacement", 1), ("replacement", u64::MAX)] {
            assert_eq!(
                new.compare_in_scopes(
                    &scope(),
                    &RepositoryContextRevision::new(epoch, sequence),
                    &scope()
                ),
                None
            );
        }
        let mut replaced_daemon = scope();
        replaced_daemon.daemon_id = "daemon-B".into();
        let mut replaced_authority = scope();
        replaced_authority.authority_scope_id = "other-caller-workspace".into();
        let mut revoked = scope();
        revoked.authority_generation += 1;
        for different in [replaced_daemon, replaced_authority, revoked] {
            assert_eq!(old.compare_in_scopes(&scope(), &new, &different), None);
            assert_eq!(new.compare_in_scopes(&different, &old, &scope()), None);
        }
    }

    #[test]
    fn resource_and_connection_scope_json_keep_kind_account_and_generation() {
        let review = ReviewTarget {
            repository: gitlab(),
            kind: RepositoryResourceKind::MergeRequest,
            number: 42,
        };
        assert_eq!(
            serde_json::to_value(&review).unwrap(),
            json!({
                "repository":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"},
                "kind":"merge-request","number":42
            })
        );
        let current = RepositoryConnectionScope {
            connection_id: "gl".into(),
            account_id: "A".into(),
            connection_generation: 1,
        };
        let mut changed_account = current.clone();
        changed_account.account_id = "B".into();
        let mut replaced = current.clone();
        replaced.connection_generation = 2;
        assert_eq!(
            BTreeSet::from([current.clone(), changed_account, replaced]).len(),
            3
        );
        assert_eq!(
            serde_json::to_value(current).unwrap(),
            json!({"connectionId":"gl","accountId":"A","connectionGeneration":1})
        );
    }

    #[test]
    fn saved_provenance_and_unavailable_outcome_have_distinct_wire_shapes() {
        assert_eq!(
            serde_json::to_value(migrated(&github("team/old"))).unwrap(),
            json!({
                "mode":"migrated-canonical",
                "target":{"provider":"github","instanceBaseUrl":"https://github.com","projectPath":"team/old"},
                "provenance":{"source":"workspace-metadata","recordId":"workspace-1","resolverVersion":"0.9.112","evidenceId":"verified-legacy-target-1"}
            })
        );
        assert_eq!(
            serde_json::to_value(resolve_review_selection(&selected("lost"), &[], None)).unwrap(),
            json!({
                "saved":{"mode":"explicit-remote","remoteName":"lost"},"noRemotes":true,
                "outcome":{"state":"repository-unavailable","reason":"no-remote","selectionRequired":true}
            })
        );
        assert_eq!(
            serde_json::to_value(resolve_review_selection(
                &selected("lost"),
                &[remote("origin", &gitlab())],
                None
            ))
            .unwrap(),
            json!({
                "saved":{"mode":"explicit-remote","remoteName":"lost"},"noRemotes":false,
                "outcome":{"state":"selection-required","reason":"missing-selected-remote"}
            })
        );
    }

    #[test]
    fn registered_roots_keep_workspace_binding_and_omit_absent_observations() {
        let root = RepositoryRootContext {
            root: RepositoryRootId {
                workspace_id: WorkspaceId::from("w1"),
                kind: RepositoryRootKind::Registered {
                    git_root_id: WorkspaceGitRootId::from("r1"),
                },
            },
            branch: None,
            head_sha: None,
            remotes: vec![],
            targets: vec![],
            review_selection: resolve_review_selection(&SavedReviewSelection::Automatic, &[], None),
        };
        let value = serde_json::to_value(&root).unwrap();
        assert_eq!(
            value["root"],
            json!({"workspaceId":"w1","kind":"registered","gitRootId":"r1"})
        );
        assert!(value.get("branch").is_none());
        assert!(value.get("headSha").is_none());
        let restored: RepositoryRootContext = serde_json::from_value(value).unwrap();
        assert_eq!(restored, root);
        let mut foreign = root.root.clone();
        foreign.workspace_id = WorkspaceId::from("w2");
        assert_ne!(root.root, foreign);
    }

    #[test]
    fn repository_context_matches_the_shared_consumer_fixture() {
        let target = gitlab();
        let mut origin = remote("origin", &target);
        origin.push.push(RepositoryRemoteEndpoint {
            url: "git@git.example:team/sub/app.git".into(),
            resolution: RepositoryEndpointResolution::Resolved {
                target: target.clone(),
            },
        });
        let context = RepositoryContext {
            revision: RepositoryContextRevision::new("daemon-boot-1", 12),
            scope: scope(),
            roots: vec![RepositoryRootContext {
                root: RepositoryRootId {
                    workspace_id: WorkspaceId::from("workspace-1"),
                    kind: RepositoryRootKind::Primary,
                },
                branch: Some("feature".into()),
                head_sha: Some("local-B".into()),
                review_selection: resolve_review_selection(
                    &selected("origin"),
                    &[origin.clone()],
                    None,
                ),
                remotes: vec![origin],
                targets: vec![RepositoryTargetContext {
                    target,
                    provider_project_id: Some("42".into()),
                    connection: Some(RepositoryConnectionScope {
                        connection_id: "gitlab-connection".into(),
                        account_id: "account-A".into(),
                        connection_generation: 3,
                    }),
                    availability: RepositoryAvailability::Connected,
                    capabilities: vec![
                        RepositoryCapability {
                            operation: RepositoryOperation::ReadReview,
                            state: RepositoryCapabilityState::Available,
                        },
                        RepositoryCapability {
                            operation: RepositoryOperation::CreateReview,
                            state: RepositoryCapabilityState::Unknown,
                        },
                    ],
                }],
            }],
        };
        let fixture = json!({
            "revision":{"epoch":"daemon-boot-1","sequence":12},
            "scope":{"daemonId":"daemon-A","authorityScopeId":"caller-workspace-1","authorityGeneration":7},
            "roots":[{
                "root":{"workspaceId":"workspace-1","kind":"primary"},"branch":"feature","headSha":"local-B",
                "remotes":[{"name":"origin","fetch":[{"url":"https://git.example:8443/gitlab/team/sub/app.git","resolution":{"state":"resolved","target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"}}}],"push":[{"url":"git@git.example:team/sub/app.git","resolution":{"state":"resolved","target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"}}}]}],
                "targets":[{"target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"},"providerProjectId":"42","connection":{"connectionId":"gitlab-connection","accountId":"account-A","connectionGeneration":3},"availability":"connected","capabilities":[{"operation":"read-review","state":"available"},{"operation":"create-review","state":"unknown"}]}],
                "reviewSelection":{"saved":{"mode":"explicit-remote","remoteName":"origin"},"noRemotes":false,"outcome":{"state":"resolved","target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"},"source":"explicit-remote"}}
            }]
        });
        assert_eq!(serde_json::to_value(&context).unwrap(), fixture);
        let restored: RepositoryContext = serde_json::from_value(fixture).unwrap();
        assert_eq!(restored, context);
        assert_eq!(restored.compare_revision(&context), Some(Ordering::Equal));
    }

    #[test]
    fn mixed_connections_and_disconnected_targets_do_not_change_identity_selection() {
        let gh = github("team/app");
        let gl = gitlab();
        let remotes = [remote("upstream", &gh), remote("origin", &gl)];
        let target_context = RepositoryTargetContext {
            target: gl.clone(),
            provider_project_id: None,
            connection: None,
            availability: RepositoryAvailability::Disconnected,
            capabilities: vec![RepositoryCapability {
                operation: RepositoryOperation::ReadReview,
                state: RepositoryCapabilityState::Unavailable,
            }],
        };
        assert_target(
            &resolve_review_selection(&selected("origin"), &remotes, None),
            &gl,
            ReviewSelectionSource::ExplicitRemote,
        );
        let value = serde_json::to_value(target_context).unwrap();
        assert_eq!(value["availability"], "disconnected");
        assert!(value.get("connection").is_none());
        assert!(value.get("providerProjectId").is_none());
        assert_eq!(value["capabilities"][0]["state"], "unavailable");
    }
}
