//! Fresh original-account eligibility for the separately authorized, pre-ACK
//! history bootstrap. Stored correlations constrain this admission, not grant it.
use std::sync::Arc;

use super::{AdmissionError, AdmissionResult, VerifiedReviewHistory};
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    self as credentials, RepositoryCredentialError, RepositoryCredentialUse,
};
use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;
use crate::Services;
use intent_core::{AgentStatus, ExecutionScope, RepositoryProvider};
use intent_store::{RepositoryLifecycleKey, RepositoryWorkspaceAuthoritySnapshot};

/// Local pre-load transfer only. It does not confirm a callback or authorize
/// continued native execution after the pending owner has been consumed.
pub(crate) struct AdmittedReviewHistory {
    verified: Arc<VerifiedReviewHistory>,
    sources: Vec<Arc<RepositoryReadEligibility>>,
}

impl AdmittedReviewHistory {
    pub(crate) fn bind_load(
        self,
        target: intent_acp::review_monitoring::history::ReviewHistoryLoadTarget,
    ) -> Box<dyn intent_acp::review_monitoring::history::ReviewHistoryLoadAdmission> {
        Box::new(LoadAdmission {
            original: self,
            target,
        })
    }

    /// Enroll only the allocation already attached to this original pending
    /// owner, under the same registry/source lock order as final transfer.
    pub(crate) fn retain_native_context(&self) -> AdmissionResult<()> {
        let native = self.verified.original.native_allocation()?;
        self.verified.with_current(|| {
            if native.is_retired() {
                return Err(AdmissionError::Retired);
            }
            if self.sources.is_empty() {
                return Ok(());
            }
            let originals = self.sources.iter().map(Arc::as_ref).collect::<Vec<_>>();
            RepositoryReadEligibility::retain_native_context(&originals, &native)
                .map_err(|_| AdmissionError::Retired)
        })
    }

    pub(crate) fn with_current<T: Send>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T> + Send,
    ) -> AdmissionResult<T> {
        self.verified.with_current(|| {
            if self.sources.is_empty() {
                return action();
            }
            let sources = self.sources.iter().map(Arc::as_ref).collect::<Vec<_>>();
            let mut result = None;
            RepositoryReadEligibility::with_all_current_and_facts(&sources, None, |_| {
                result = Some(action());
                Ok(())
            })
            .map_err(|_| AdmissionError::Retired)?;
            result.ok_or(AdmissionError::Retired)?
        })
    }
}

struct LoadAdmission {
    original: AdmittedReviewHistory,
    target: intent_acp::review_monitoring::history::ReviewHistoryLoadTarget,
}
impl intent_acp::review_monitoring::history::ReviewHistoryLoadAdmission for LoadAdmission {
    fn admit(
        &self,
        boundary: &intent_acp::review_monitoring::history::ReviewHistoryLoadBoundary,
        packet: intent_acp::review_monitoring::history::PreparedReviewHistoryLoad<'_>,
    ) -> intent_acp::error::AcpResult<
        intent_acp::review_monitoring::history::ReviewHistoryLoadReceipt,
    > {
        if !self.target.matches(boundary) {
            return Err(intent_acp::error::AcpError::Protocol(
                "Review monitoring context unavailable".into(),
            ));
        }
        self.original
            .with_current(|| Ok(packet.transfer(boundary)))
            .map_err(|_| {
                intent_acp::error::AcpError::Protocol(
                    "Review monitoring context unavailable".into(),
                )
            })?
    }
}

struct Authority {
    services: Arc<Services>,
    verified: Arc<VerifiedReviewHistory>,
    workspace: RepositoryWorkspaceAuthoritySnapshot,
    expected: RepositoryAuthorityRequest,
}
struct Fence(Arc<Authority>);

impl RepositoryAuthorityFence for Fence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> credentials::Result<()> + Send),
    ) -> credentials::Result<()> {
        self.0
            .verified
            .with_current(|| Ok(action()))
            .map_err(|_| RepositoryCredentialError::Retired)?
    }
}

impl RepositoryAuthority for Arc<Authority> {
    fn revalidate<'a>(
        &'a self,
        expected: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if expected != &self.expected {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            let current = facts(&self.verified, &self.services)
                .await
                .map_err(|_| RepositoryCredentialError::AuthorityDenied)?;
            if current != self.workspace {
                return Err(RepositoryCredentialError::Retired);
            }
            Ok(Box::new(Fence(self.clone())) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

async fn facts(
    verified: &VerifiedReviewHistory,
    services: &Services,
) -> AdmissionResult<RepositoryWorkspaceAuthoritySnapshot> {
    let original = &verified.original;
    if !original
        .store
        .shares_repository_lifecycle_domain(&services.store)
    {
        return Err(AdmissionError::Denied);
    }
    original.with_current(|| Ok(()))?;
    let before = original
        .store
        .repository_workspace_authority_snapshot(&original.workspace)
        .await
        .map_err(|_| AdmissionError::Unavailable)?;
    let agent = original
        .store
        .get_agent_session_summary(&original.agent)
        .await
        .map_err(|_| AdmissionError::Denied)?;
    if before.workspace.value.is_none()
        || before.workspace.revision.is_none()
        || agent.workspace_id != original.workspace
        || agent.acp_session_id != original.session
        || agent.retired_at.is_some()
        || agent.status == AgentStatus::Deleted
        || services
            .pending_agent_deletes
            .deadline(&crate::delete_grace::PendingDeleteSubject::Agent {
                workspace_id: original.workspace.clone(),
                agent_id: original.agent.clone(),
            })
            .map_err(|_| AdmissionError::Unavailable)?
            .is_some()
    {
        return Err(AdmissionError::Denied);
    }
    let after = original
        .store
        .repository_workspace_authority_snapshot(&original.workspace)
        .await
        .map_err(|_| AdmissionError::Unavailable)?;
    original.with_current(|| Ok(()))?;
    if before != after {
        return Err(AdmissionError::BindingChanged);
    }
    Ok(before)
}

impl VerifiedReviewHistory {
    pub(crate) async fn admit(
        self,
        services: Arc<Services>,
    ) -> AdmissionResult<AdmittedReviewHistory> {
        // Also fence host authorization facts read below. This subscription does
        // not create a Wire caller or turn workspace ownership into forge access.
        self.original
            .subscribe(&[RepositoryLifecycleKey::WireAuthority])?;
        let verified = Arc::new(self);
        let workspace = facts(&verified, &services).await?;
        let mut sources = Vec::with_capacity(verified.envelopes.len());
        for envelope in &verified.envelopes {
            let original = &envelope.binding;
            if original.resource.repository.provider != RepositoryProvider::Gitlab {
                return Err(AdmissionError::Denied);
            }
            let settled = services
                .gitlab_repository_settled_connection()
                .map_err(|_| AdmissionError::Unavailable)?;
            let selected = settled.selected();
            if selected.binding.scope.account_id != original.connection.account_id {
                return Err(AdmissionError::Denied);
            }
            let expected = RepositoryAuthorityRequest {
                execution: ExecutionScope {
                    daemon_id: services.daemon_boot_id.clone(),
                    authority_scope_id: uuid::Uuid::new_v4().to_string(),
                    authority_generation: workspace
                        .workspace
                        .revision
                        .ok_or(AdmissionError::Denied)?
                        .get(),
                },
                target: original.resource.repository.clone(),
                connection: selected.binding.scope.clone(),
                use_kind: RepositoryCredentialUse::NativeRead,
                allowed_transport: RepositoryCredentialTransport::GitlabApi(
                    settled.descriptor().clone(),
                ),
            };
            let authority = Arc::new(Authority {
                services: services.clone(),
                verified: verified.clone(),
                workspace: workspace.clone(),
                expected: expected.clone(),
            });
            let admission = services
                .repository_connection_directory()
                .admit(
                    &selected.binding,
                    expected.clone(),
                    Arc::new(authority.clone()),
                )
                .map_err(|_| AdmissionError::Denied)?;
            let eligibility = Arc::new(
                services
                    .gitlab_repository_read_eligibility(&admission)
                    .map_err(|_| AdmissionError::Denied)?,
            );
            authority
                .revalidate(&expected)
                .await
                .map_err(|_| AdmissionError::Denied)?
                .dispatch(&mut || eligibility.check())
                .map_err(|_| AdmissionError::Retired)?;
            sources.push(eligibility);
        }
        let admitted = AdmittedReviewHistory { verified, sources };
        admitted.with_current(|| Ok(()))?;
        Ok(admitted)
    }
}
