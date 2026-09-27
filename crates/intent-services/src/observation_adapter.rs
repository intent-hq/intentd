//! Eligibility for canonical repository scopes and typed provider evidence.
//!
//! The existing cache owner supplies admitted identities, keeps one resource
//! slot per key, and holds its lock across completion and payload replacement.
//! This adapter retains no payloads, pages, credentials or quota timers. Its
//! project registry contains only weak references to live eligibility handles.
//! A receipt still requires independent caller admission and the cache's TTL.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};

use intent_core::{
    ExecutionScope, RepositoryConnectionScope, RepositoryResourceKind, RepositoryTarget,
    ReviewTarget,
};
use intent_sourcecontrol::{
    error::{ProviderFailure, ProviderFailureKind},
    Error, ProviderAvailability, RateLimitStatus, ReviewObservation,
};

use super::observation_policy::{
    Coverage, EligibleRead, Ineligible, ObservationKey, ObservationScope, ObservationSlot,
    ReadTicket, ScopeTicket,
};

pub(crate) type QualifiedScope = (ExecutionScope, RepositoryConnectionScope);
pub(crate) type QualifiedKey = ObservationKey<QualifiedScope, ReviewTarget>;
type ProjectScopes = HashMap<RepositoryTarget, Weak<ProjectState>>;

/// A bounded window of metadata, not payloads or permanent item tombstones.
/// A list that falls behind the window must retry; retained row receipts use
/// their cache slot instead and are not revoked by unrelated history traffic.
const SUMMARY_HISTORY_LIMIT: usize = 256;

#[derive(Debug, Default)]
struct SummaryHistory {
    sequence: u64,
    discarded_through: u64,
    exhausted: bool,
    events: VecDeque<SummaryEvent>,
}

#[derive(Debug)]
struct SummaryEvent {
    sequence: u64,
    target: ReviewTarget,
    change: SummaryChange,
}

#[derive(Debug, Clone, Copy)]
enum SummaryChange {
    Denied,
    Evicted,
    Observed { started: u64 },
}

impl SummaryHistory {
    fn next(&mut self) -> Result<u64, Ineligible> {
        self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
            self.exhausted = true;
            Ineligible::SequenceExhausted
        })?;
        Ok(self.sequence)
    }

    fn record(&mut self, target: ReviewTarget, change: SummaryChange) {
        let Ok(sequence) = self.next() else { return };
        if self.events.len() == SUMMARY_HISTORY_LIMIT {
            self.discarded_through = self.events.pop_front().unwrap().sequence;
        }
        self.events.push_back(SummaryEvent {
            sequence,
            target,
            change,
        });
    }

    fn validate_start(&self, started: u64) -> Result<(), Ineligible> {
        if self.exhausted {
            return Err(Ineligible::SequenceExhausted);
        }
        if started <= self.discarded_through {
            return Err(Ineligible::HistoryExpired);
        }
        Ok(())
    }

    fn validate(&self, started: u64, target: &ReviewTarget) -> Result<(), Ineligible> {
        self.validate_start(started)?;
        for event in &self.events {
            if event.sequence <= started || &event.target != target {
                continue;
            }
            match event.change {
                SummaryChange::Denied => return Err(Ineligible::DeniedSinceRequest),
                SummaryChange::Evicted => return Err(Ineligible::DifferentSlot),
                SummaryChange::Observed { started: newer } if newer > started => {
                    return Err(Ineligible::OlderObservation);
                }
                SummaryChange::Observed { .. } => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ProjectState {
    scope: ObservationScope<RepositoryTarget>,
    lists: [ObservationScope<RepositoryResourceKind>; 3],
    history: Mutex<SummaryHistory>,
}

impl ProjectState {
    fn list(&self, kind: RepositoryResourceKind) -> &ObservationScope<RepositoryResourceKind> {
        &self.lists[match kind {
            RepositoryResourceKind::PullRequest => 0,
            RepositoryResourceKind::MergeRequest => 1,
            RepositoryResourceKind::Issue => 2,
        }]
    }
}

/// One server-owned lifetime for an admitted authority and connection.
/// Reuse clones within that lifetime; retire before account/backend replacement.
#[derive(Debug, Clone)]
pub(crate) struct ConnectionObservations {
    scope: ObservationScope<QualifiedScope>,
    projects: Arc<Mutex<ProjectScopes>>,
}

impl ConnectionObservations {
    pub(crate) fn new(execution: ExecutionScope, connection: RepositoryConnectionScope) -> Self {
        Self {
            scope: ObservationScope::new((execution, connection)),
            projects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn retire(&self) {
        self.scope.retire();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.scope.is_active()
    }

    #[cfg(test)]
    pub(crate) fn retained_project_count(&self) -> usize {
        self.projects
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.strong_count() > 0)
            .count()
    }

    pub(crate) fn key(&self, resource: ReviewTarget) -> QualifiedKey {
        ObservationKey {
            scope: self.scope.key().clone(),
            resource,
        }
    }

    pub(crate) fn project(&self, repository: RepositoryTarget) -> ProjectObservations {
        let mut projects = self.projects.lock().unwrap();
        // Expired keys do not become a history of every project ever observed.
        projects.retain(|_, scope| scope.strong_count() > 0);
        let scope = if let Some(scope) = projects.get(&repository).and_then(Weak::upgrade) {
            scope
        } else {
            let scope = Arc::new(ProjectState {
                scope: ObservationScope::new(repository.clone()),
                lists: [
                    RepositoryResourceKind::PullRequest,
                    RepositoryResourceKind::MergeRequest,
                    RepositoryResourceKind::Issue,
                ]
                .map(ObservationScope::new),
                history: Mutex::new(SummaryHistory::default()),
            });
            projects.insert(repository.clone(), Arc::downgrade(&scope));
            scope
        };
        ProjectObservations {
            repository,
            connection: self.scope.clone(),
            scope,
        }
    }
}

/// All retained and in-flight resources in the addressed canonical project.
#[derive(Debug, Clone)]
pub(crate) struct ProjectObservations {
    repository: RepositoryTarget,
    connection: ObservationScope<QualifiedScope>,
    scope: Arc<ProjectState>,
}

impl ProjectObservations {
    pub(crate) fn slot(&self, kind: RepositoryResourceKind, number: u64) -> ReviewObservations {
        let target = ReviewTarget {
            repository: self.repository.clone(),
            kind,
            number,
        };
        ReviewObservations {
            item: ObservationSlot::new(&self.connection, target.clone()),
            project: ObservationSlot::new(&self.scope.scope, target),
            connection_scope: self.connection.clone(),
            project_scope: self.scope.clone(),
        }
    }

    pub(crate) fn begin_list(
        &self,
        kind: RepositoryResourceKind,
    ) -> Result<ListObservations, Ineligible> {
        Ok(ListObservations {
            connection_ticket: self.connection.begin()?,
            project_ticket: self.scope.scope.begin()?,
            list_ticket: self.scope.list(kind).begin()?,
            started: self.scope.history.lock().unwrap().next()?,
            project: self.clone(),
            kind,
        })
    }
}

/// Captures authority, connection, project, endpoint and ordering BEFORE await.
/// Each explicitly admitted project in a blended page needs its own capture;
/// returned URLs and current workspace defaults are never evidence of scope.
#[derive(Debug, Clone)]
pub(crate) struct ListObservations {
    project: ProjectObservations,
    kind: RepositoryResourceKind,
    connection_ticket: ScopeTicket,
    project_ticket: ScopeTicket,
    list_ticket: ScopeTicket,
    started: u64,
}

/// Retained rows need only weak project ownership plus their original denial
/// stamps. Evicted/copied receipts cannot pin a project's history indefinitely.
#[derive(Debug)]
pub(crate) struct ListReceipt {
    project: Weak<ProjectState>,
    repository: RepositoryTarget,
    kind: RepositoryResourceKind,
    connection_ticket: ScopeTicket,
    project_ticket: ScopeTicket,
    list_ticket: ScopeTicket,
}

impl ListReceipt {
    pub(crate) fn matches(
        &self,
        connection: &ConnectionObservations,
        repository: &RepositoryTarget,
        kind: RepositoryResourceKind,
    ) -> bool {
        &self.repository == repository
            && self.kind == kind
            && connection
                .scope
                .validate_owner(&self.connection_ticket)
                .is_ok()
    }

    pub(crate) fn validate(&self, connection: &ConnectionObservations) -> Result<(), Ineligible> {
        connection.scope.validate(&self.connection_ticket)?;
        let project = self.project.upgrade().ok_or(Ineligible::DifferentSlot)?;
        project.scope.validate(&self.project_ticket)?;
        project.list(self.kind).validate(&self.list_ticket)
    }
}

impl ListObservations {
    pub(crate) fn receipt(&self) -> ListReceipt {
        ListReceipt {
            project: Arc::downgrade(&self.project.scope),
            repository: self.project.repository.clone(),
            kind: self.kind,
            connection_ticket: self.connection_ticket.clone(),
            project_ticket: self.project_ticket.clone(),
            list_ticket: self.list_ticket.clone(),
        }
    }
    pub(crate) fn matches(
        &self,
        connection: &ConnectionObservations,
        repository: &RepositoryTarget,
        kind: RepositoryResourceKind,
    ) -> bool {
        &self.project.repository == repository
            && self.kind == kind
            && connection
                .scope
                .validate_owner(&self.connection_ticket)
                .is_ok()
    }

    pub(crate) fn validate_scope(&self) -> Result<(), Ineligible> {
        self.project.connection.validate(&self.connection_ticket)?;
        self.project.scope.scope.validate(&self.project_ticket)?;
        self.project
            .scope
            .list(self.kind)
            .validate(&self.list_ticket)
    }

    pub(crate) fn validate_start(&self) -> Result<(), Ineligible> {
        self.validate_scope()?;
        self.project
            .scope
            .history
            .lock()
            .unwrap()
            .validate_start(self.started)
    }

    pub(crate) fn validate_row(&self, target: &ReviewTarget) -> Result<(), Ineligible> {
        self.validate_scope()?;
        if target.repository != self.project.repository || target.kind != self.kind {
            return Err(Ineligible::DifferentSlot);
        }
        self.project
            .scope
            .history
            .lock()
            .unwrap()
            .validate(self.started, target)
    }

    pub(crate) fn observed(&self, target: ReviewTarget) {
        self.project.scope.history.lock().unwrap().record(
            target,
            SummaryChange::Observed {
                started: self.started,
            },
        );
    }

    /// A list-endpoint denial revokes only that list coverage. It says nothing
    /// about a specific item or the parent project. Only the provider's explicit
    /// `ProjectDenied` / `CredentialRejected` evidence has that broader meaning.
    pub(crate) fn failure(&self, error: Error) -> Result<Error, Ineligible> {
        self.project
            .connection
            .validate_owner(&self.connection_ticket)?;
        self.project
            .scope
            .scope
            .validate_owner(&self.project_ticket)?;
        match &error {
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::CredentialRejected,
                ..
            }) => self.project.connection.deny(),
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::ProjectDenied,
                ..
            }) => self.project.scope.scope.deny(),
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::ResourceDenied,
                ..
            }) => self.project.scope.list(self.kind).deny(),
            _ => {}
        }
        Ok(error)
    }
}

/// Both stamps are captured before the provider await, never on completion.
#[derive(Debug)]
pub(crate) struct ObservationTicket {
    item: ReadTicket,
    project: ReadTicket,
}

#[derive(Debug)]
pub(crate) struct ObservationReceipt {
    item: EligibleRead,
    project: EligibleRead,
}

/// A partial observation has no complete-cache receipt. Its unchanged quota
/// evidence must still reach the existing connection backoff owner, including
/// when a rate-limited optional field accompanies a successful primary read.
#[derive(Debug)]
pub(crate) struct SnapshotCompletion {
    pub(crate) observation: ReviewObservation,
    pub(crate) quota: RateLimitStatus,
    pub(crate) receipt: Option<ObservationReceipt>,
}

#[derive(Debug)]
pub(crate) struct ReviewObservations {
    item: ObservationSlot<QualifiedScope, ReviewTarget>,
    project: ObservationSlot<RepositoryTarget, ReviewTarget>,
    connection_scope: ObservationScope<QualifiedScope>,
    project_scope: Arc<ProjectState>,
}

impl ReviewObservations {
    pub(crate) fn belongs_to(&self, current: &ConnectionObservations) -> bool {
        self.item.belongs_to(&current.scope)
    }

    pub(crate) fn validate(&self, ticket: &ObservationTicket) -> Result<(), Ineligible> {
        self.item.validate(&ticket.item)?;
        self.project.validate(&ticket.project)
    }

    pub(crate) fn key(&self) -> &ObservationKey<QualifiedScope, ReviewTarget> {
        self.item.key()
    }

    pub(crate) fn begin(&mut self, coverage: Coverage) -> Result<ObservationTicket, Ineligible> {
        Ok(ObservationTicket {
            item: self.item.begin(coverage)?,
            project: self.project.begin(coverage)?,
        })
    }

    /// For an authorized primary-resource success, not optional-field success.
    /// Eligibility records an observation; it does not assert passing checks.
    pub(crate) fn primary_success(
        &mut self,
        ticket: ObservationTicket,
    ) -> Result<ObservationReceipt, Ineligible> {
        self.validate(&ticket)?;
        Ok(ObservationReceipt {
            item: self.item.success(ticket.item)?,
            project: self.project.success(ticket.project)?,
        })
    }

    /// Partial evidence updates response ordering without granting freshness.
    pub(crate) fn partial_success(&mut self, ticket: &ObservationTicket) -> Result<(), Ineligible> {
        self.validate(ticket)?;
        self.item.observe(&ticket.item)?;
        self.project.observe(&ticket.project)
    }

    /// Apply only provider-owned denial breadth for the captured target.
    /// Status codes and generic credential/admission errors are not classified
    /// here. Endpoint-local denial never becomes whole-project denial.
    pub(crate) fn failure(
        &mut self,
        ticket: ObservationTicket,
        error: Error,
        quota: RateLimitStatus,
    ) -> Result<(Error, RateLimitStatus), Ineligible> {
        let error = self.item.failure(ticket.item, error, |error| {
            matches!(
                error,
                Error::Provider(ProviderFailure {
                    kind: ProviderFailureKind::ResourceDenied,
                    ..
                })
            )
        })?;
        let error = self.project.failure(ticket.project, error, |_| false)?;
        // Both private tickets must belong to this active slot before broad
        // evidence can invalidate any of its siblings.
        match &error {
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::ResourceDenied,
                ..
            }) => self
                .project_scope
                .history
                .lock()
                .unwrap()
                .record(self.key().resource.clone(), SummaryChange::Denied),
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::CredentialRejected,
                ..
            }) => self.connection_scope.deny(),
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::ProjectDenied,
                ..
            }) => self.project_scope.scope.deny(),
            _ => {}
        }
        Ok((error, quota))
    }

    /// Preserve the provider's partial/unknown fields and quota evidence.
    /// Without a receipt the result must not replace a complete cached payload
    /// or refresh its timestamp. This is not a response/authorization adapter.
    pub(crate) fn snapshot(
        &mut self,
        ticket: ObservationTicket,
        observation: ReviewObservation,
        quota: RateLimitStatus,
    ) -> Result<SnapshotCompletion, Ineligible> {
        let receipt = if complete_snapshot(&observation) {
            Some(self.primary_success(ticket)?)
        } else {
            self.partial_success(&ticket)?;
            None
        };
        Ok(SnapshotCompletion {
            observation,
            quota,
            receipt,
        })
    }

    pub(crate) fn can_serve(
        &self,
        receipt: &ObservationReceipt,
        current: &ConnectionObservations,
    ) -> bool {
        self.item.can_serve(&receipt.item, &current.scope)
            && self
                .project
                .can_serve(&receipt.project, &self.project_scope.scope)
    }

    /// Called when the actual cache slot is evicted/replaced, including a slot
    /// created after a pending list began. There is no retained payload here.
    pub(crate) fn evicted(&self) {
        self.project_scope
            .history
            .lock()
            .unwrap()
            .record(self.key().resource.clone(), SummaryChange::Evicted);
    }
}

pub(crate) fn complete_snapshot(observation: &ReviewObservation) -> bool {
    [
        observation.availability.policy,
        observation.availability.approvals,
        observation.availability.checks,
        observation.availability.discussions,
    ]
    .into_iter()
    .all(|state| state == ProviderAvailability::Available)
        && observation.reviews.is_some()
        && observation.threads.is_some()
        && observation.conversation_count.is_some()
        && observation.signals.checks_known
}
