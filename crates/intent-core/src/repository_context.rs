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

// These new opaque counters must retain every bit across JavaScript transports.
// Numeric JSON and noncanonical decimal spellings are deliberately rejected.
mod decimal_u64 {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde(with) requires a reference to the field"
    )]
    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || value.len() > 20
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(D::Error::custom("expected a canonical decimal u64 string"));
        }
        value.parse().map_err(D::Error::custom)
    }
}

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
    /// Canonical decimal string on the wire, preserving the full `u64` range.
    #[serde(with = "decimal_u64")]
    pub authority_generation: u64,
}

/// Non-secret repository account scope; combine with `ExecutionScope` for keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryConnectionScope {
    pub connection_id: String,
    pub account_id: String,
    /// Canonical decimal string on the wire, preserving the full `u64` range.
    #[serde(with = "decimal_u64")]
    pub connection_generation: u64,
}

/// Opaque revision token. No global ordering exists across scopes or epochs.
///
/// Producers advance the sequence on observed context changes and replace the
/// epoch at restart. Consumers must use `compare_in_scopes`, not order serialized
/// tokens or treat revision freshness as authorization. The sequence serializes
/// as a canonical decimal string so JavaScript transports cannot round it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextRevision {
    epoch: String,
    #[serde(with = "decimal_u64")]
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
    /// Retained historical intent whose canonical target has not been proved.
    /// Producers supply only source/record facts they actually possess; missing
    /// facts stay absent. Neither these fields nor deserialization prove intent.
    UnresolvedHistorical {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<HistoricalTargetSource>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        record_id: Option<String>,
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
    UnresolvedHistoricalChoice,
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
            SavedReviewSelection::UnresolvedHistorical { .. } => {
                ReviewSelectionOutcome::SelectionRequired {
                    reason: ReviewSelectionRequiredReason::UnresolvedHistoricalChoice,
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
    /// Insufficient verified connection facts, not known absence or denial.
    Unknown,
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
    fn new_counters_serialize_as_canonical_decimal_strings() {
        for value in [0, 1, 9_007_199_254_740_993, u64::MAX] {
            let revision = RepositoryContextRevision::new("boot", value);
            let mut execution = scope();
            execution.authority_generation = value;
            let connection = RepositoryConnectionScope {
                connection_id: "gl".into(),
                account_id: "account-A".into(),
                connection_generation: value,
            };
            let expected_revision = json!({"epoch":"boot","sequence":value.to_string()});
            let expected_execution = json!({
                "daemonId":"daemon-A","authorityScopeId":"caller-workspace-1",
                "authorityGeneration":value.to_string()
            });
            let expected_connection = json!({
                "connectionId":"gl","accountId":"account-A",
                "connectionGeneration":value.to_string()
            });
            assert_eq!(serde_json::to_value(&revision).unwrap(), expected_revision);
            assert_eq!(
                serde_json::to_value(&execution).unwrap(),
                expected_execution
            );
            assert_eq!(
                serde_json::to_value(&connection).unwrap(),
                expected_connection
            );
            assert_eq!(
                serde_json::from_value::<RepositoryContextRevision>(expected_revision).unwrap(),
                revision
            );
            assert_eq!(
                serde_json::from_value::<ExecutionScope>(expected_execution).unwrap(),
                execution
            );
            assert_eq!(
                serde_json::from_value::<RepositoryConnectionScope>(expected_connection).unwrap(),
                connection
            );
        }
    }

    #[test]
    fn new_counters_reject_numbers_noncanonical_strings_and_overflow() {
        for invalid in [
            json!(0),
            json!(1),
            json!(9_007_199_254_740_993_u64),
            json!(null),
            json!(false),
            json!(""),
            json!("+1"),
            json!("-1"),
            json!("01"),
            json!("00"),
            json!(" 1"),
            json!("1 "),
            json!("1.0"),
            json!("1e3"),
            json!("1_000"),
            json!("١"),
            json!("18446744073709551616"),
        ] {
            assert!(
                serde_json::from_value::<RepositoryContextRevision>(json!({
                    "epoch":"boot","sequence":invalid
                }))
                .is_err(),
                "revision accepted {invalid}"
            );
            assert!(serde_json::from_value::<ExecutionScope>(json!({
                "daemonId":"daemon-A","authorityScopeId":"caller-workspace-1","authorityGeneration":invalid
            })).is_err(), "authority accepted {invalid}");
            assert!(
                serde_json::from_value::<RepositoryConnectionScope>(json!({
                    "connectionId":"gl","accountId":"account-A","connectionGeneration":invalid
                }))
                .is_err(),
                "connection accepted {invalid}"
            );
        }
    }

    #[test]
    fn decimal_counter_ordering_keeps_adjacent_values_above_js_safe_integer() {
        let before: RepositoryContextRevision = serde_json::from_value(json!({
            "epoch":"boot","sequence":"9007199254740992"
        }))
        .unwrap();
        let after: RepositoryContextRevision = serde_json::from_value(json!({
            "epoch":"boot","sequence":"9007199254740993"
        }))
        .unwrap();
        assert_ne!(before, after);
        assert_eq!(
            before.compare_in_scopes(&scope(), &after, &scope()),
            Some(Ordering::Less)
        );
        assert_eq!(
            after.compare_in_scopes(&scope(), &before, &scope()),
            Some(Ordering::Greater)
        );
        let mut before_scope = scope();
        before_scope.authority_generation = 9_007_199_254_740_992;
        let mut after_scope = before_scope.clone();
        after_scope.authority_generation += 1;
        assert_eq!(
            before.compare_in_scopes(&before_scope, &after, &after_scope),
            None
        );
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
            json!({"connectionId":"gl","accountId":"A","connectionGeneration":"1"})
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
        let mut execution_scope = scope();
        execution_scope.authority_generation = 9_007_199_254_740_995;
        let mut origin = remote("origin", &target);
        origin.push.push(RepositoryRemoteEndpoint {
            url: "git@git.example:team/sub/app.git".into(),
            resolution: RepositoryEndpointResolution::Resolved {
                target: target.clone(),
            },
        });
        let github_target = github("team/app");
        let remotes = vec![origin, remote("upstream", &github_target)];
        let context = RepositoryContext {
            revision: RepositoryContextRevision::new("daemon-boot-1", 9_007_199_254_740_993),
            scope: execution_scope,
            roots: vec![RepositoryRootContext {
                root: RepositoryRootId {
                    workspace_id: WorkspaceId::from("workspace-1"),
                    kind: RepositoryRootKind::Primary,
                },
                branch: Some("feature".into()),
                head_sha: Some("local-B".into()),
                review_selection: resolve_review_selection(&selected("origin"), &remotes, None),
                remotes,
                targets: vec![
                    RepositoryTargetContext {
                        target,
                        provider_project_id: Some("42".into()),
                        connection: Some(RepositoryConnectionScope {
                            connection_id: "gitlab-connection".into(),
                            account_id: "account-A".into(),
                            connection_generation: u64::MAX,
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
                    },
                    RepositoryTargetContext {
                        target: github_target,
                        provider_project_id: Some("github-project-1".into()),
                        connection: Some(RepositoryConnectionScope {
                            connection_id: "github-connection".into(),
                            account_id: "account-GH".into(),
                            connection_generation: 4,
                        }),
                        availability: RepositoryAvailability::Connected,
                        capabilities: vec![RepositoryCapability {
                            operation: RepositoryOperation::ReadReview,
                            state: RepositoryCapabilityState::Available,
                        }],
                    },
                ],
            }],
        };
        let fixture = json!({
            "revision":{"epoch":"daemon-boot-1","sequence":"9007199254740993"},
            "scope":{"daemonId":"daemon-A","authorityScopeId":"caller-workspace-1","authorityGeneration":"9007199254740995"},
            "roots":[{
                "root":{"workspaceId":"workspace-1","kind":"primary"},"branch":"feature","headSha":"local-B",
                "remotes":[{"name":"origin","fetch":[{"url":"https://git.example:8443/gitlab/team/sub/app.git","resolution":{"state":"resolved","target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"}}}],"push":[{"url":"git@git.example:team/sub/app.git","resolution":{"state":"resolved","target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"}}}]},{"name":"upstream","fetch":[{"url":"https://github.com/team/app.git","resolution":{"state":"resolved","target":{"provider":"github","instanceBaseUrl":"https://github.com","projectPath":"team/app"}}}],"push":[]}],
                "targets":[{"target":{"provider":"gitlab","instanceBaseUrl":"https://git.example:8443/gitlab","projectPath":"team/sub/app"},"providerProjectId":"42","connection":{"connectionId":"gitlab-connection","accountId":"account-A","connectionGeneration":"18446744073709551615"},"availability":"connected","capabilities":[{"operation":"read-review","state":"available"},{"operation":"create-review","state":"unknown"}]},{"target":{"provider":"github","instanceBaseUrl":"https://github.com","projectPath":"team/app"},"providerProjectId":"github-project-1","connection":{"connectionId":"github-connection","accountId":"account-GH","connectionGeneration":"4"},"availability":"connected","capabilities":[{"operation":"read-review","state":"available"}]}],
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

    #[test]
    fn unknown_availability_does_not_invent_connection_or_capability() {
        let context = RepositoryTargetContext {
            target: gitlab(),
            provider_project_id: None,
            connection: None,
            availability: RepositoryAvailability::Unknown,
            capabilities: vec![RepositoryCapability {
                operation: RepositoryOperation::ReadReview,
                state: RepositoryCapabilityState::Unknown,
            }],
        };
        let expected = json!({
            "target": gitlab(), "availability": "unknown",
            "capabilities": [{"operation": "read-review", "state": "unknown"}]
        });
        assert_eq!(serde_json::to_value(&context).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<RepositoryTargetContext>(expected).unwrap(),
            context
        );
    }

    #[test]
    fn existing_availability_and_saved_selection_wire_forms_stay_exact() {
        for (availability, expected) in [
            (RepositoryAvailability::Connected, "connected"),
            (RepositoryAvailability::Disconnected, "disconnected"),
            (RepositoryAvailability::Disabled, "disabled"),
            (RepositoryAvailability::Unsupported, "unsupported"),
        ] {
            assert_eq!(serde_json::to_value(availability).unwrap(), json!(expected));
            assert_eq!(
                serde_json::from_value::<RepositoryAvailability>(json!(expected)).unwrap(),
                availability
            );
        }
        for (saved, expected) in [
            (
                SavedReviewSelection::Automatic,
                json!({"mode": "automatic"}),
            ),
            (
                selected("upstream"),
                json!({"mode": "explicit-remote", "remoteName": "upstream"}),
            ),
            (
                migrated(&gitlab()),
                json!({
                    "mode": "migrated-canonical", "target": gitlab(),
                    "provenance": {
                        "source": "workspace-metadata", "recordId": "workspace-1",
                        "resolverVersion": "0.9.112", "evidenceId": "verified-legacy-target-1"
                    }
                }),
            ),
        ] {
            assert_eq!(serde_json::to_value(&saved).unwrap(), expected);
            assert_eq!(
                serde_json::from_value::<SavedReviewSelection>(expected).unwrap(),
                saved
            );
        }
    }

    #[test]
    fn unresolved_history_serializes_only_known_source_and_record_facts() {
        for (source, record_id, expected) in [
            (None, None, json!({"mode": "unresolved-historical"})),
            (
                Some(HistoricalTargetSource::WorkspaceMetadata),
                Some("workspace-1"),
                json!({"mode": "unresolved-historical", "source": "workspace-metadata", "recordId": "workspace-1"}),
            ),
            (
                Some(HistoricalTargetSource::RegisteredRootMetadata),
                Some("root-1"),
                json!({"mode": "unresolved-historical", "source": "registered-root-metadata", "recordId": "root-1"}),
            ),
            (
                Some(HistoricalTargetSource::WorkspaceMetadata),
                None,
                json!({"mode": "unresolved-historical", "source": "workspace-metadata"}),
            ),
            (
                None,
                Some("retained-record"),
                json!({"mode": "unresolved-historical", "recordId": "retained-record"}),
            ),
        ] {
            let saved = SavedReviewSelection::UnresolvedHistorical {
                source,
                record_id: record_id.map(str::to_owned),
            };
            assert_eq!(serde_json::to_value(&saved).unwrap(), expected);
            let restored: SavedReviewSelection = serde_json::from_value(expected).unwrap();
            assert_eq!(restored, saved);
            let resolution =
                resolve_review_selection(&restored, &[remote("origin", &gitlab())], None);
            assert_eq!(resolution.saved, saved);
            assert_eq!(
                serde_json::to_value(resolution.outcome).unwrap(),
                json!({"state": "selection-required", "reason": "unresolved-historical-choice"})
            );
        }
    }

    #[test]
    fn unresolved_history_never_heals_from_current_fetch_or_push_inventory() {
        let saved = SavedReviewSelection::UnresolvedHistorical {
            source: Some(HistoricalTargetSource::WorkspaceMetadata),
            record_id: Some("workspace-1".into()),
        };
        for remotes in [
            vec![remote("origin", &gitlab())],
            vec![remote("renamed", &gitlab())],
            vec![remote("origin", &gitlab()), remote("added", &gitlab())],
            vec![remote("origin", &gitlab()), remote("added", &github("o/r"))],
            vec![RepositoryRemote {
                name: "origin".into(),
                fetch: vec![],
                push: vec![endpoint(&gitlab())],
            }],
            vec![RepositoryRemote {
                name: "origin".into(),
                fetch: vec![endpoint(&github("o/r"))],
                push: vec![endpoint(&gitlab())],
            }],
        ] {
            let resolution = resolve_review_selection(&saved, &remotes, None);
            assert!(!resolution.no_remotes);
            assert_eq!(resolution.saved, saved);
            assert_required(
                &resolution,
                ReviewSelectionRequiredReason::UnresolvedHistoricalChoice,
            );
        }
    }

    #[test]
    fn unresolved_history_without_remotes_retains_required_choice() {
        let saved = SavedReviewSelection::UnresolvedHistorical {
            source: None,
            record_id: None,
        };
        let resolution = resolve_review_selection(&saved, &[], None);
        assert_eq!(resolution.saved, saved);
        assert!(resolution.no_remotes);
        assert_eq!(
            resolution.outcome,
            ReviewSelectionOutcome::RepositoryUnavailable {
                reason: RepositoryUnavailableReason::NoRemote,
                selection_required: true,
            }
        );
    }

    #[test]
    fn unresolved_history_allows_per_call_override_without_rewriting_saved_intent() {
        let saved = SavedReviewSelection::UnresolvedHistorical {
            source: Some(HistoricalTargetSource::RegisteredRootMetadata),
            record_id: Some("root-1".into()),
        };
        for remotes in [vec![], vec![remote("origin", &github("o/r"))]] {
            let before = resolve_review_selection(&saved, &remotes, None);
            let explicit = resolve_review_selection(&saved, &remotes, Some(&gitlab()));
            assert_target(&explicit, &gitlab(), ReviewSelectionSource::ExplicitCall);
            assert_eq!(explicit.saved, saved);
            assert_eq!(explicit.no_remotes, remotes.is_empty());
            assert_eq!(before, resolve_review_selection(&saved, &remotes, None));
        }
    }

    #[test]
    fn unresolved_history_requires_explicit_reset_to_restore_automatic_selection() {
        let mut saved = SavedReviewSelection::UnresolvedHistorical {
            source: None,
            record_id: None,
        };
        let remotes = [remote("origin", &gitlab())];
        assert_required(
            &resolve_review_selection(&saved, &remotes, None),
            ReviewSelectionRequiredReason::UnresolvedHistoricalChoice,
        );
        saved = SavedReviewSelection::Automatic;
        let resolution = resolve_review_selection(&saved, &remotes, None);
        assert_target(&resolution, &gitlab(), ReviewSelectionSource::Automatic);
        assert_eq!(resolution.saved, SavedReviewSelection::Automatic);
        assert_required(
            &resolve_review_selection(
                &saved,
                &[remotes[0].clone(), remote("upstream", &github("o/r"))],
                None,
            ),
            ReviewSelectionRequiredReason::AmbiguousTargets,
        );
    }
}
