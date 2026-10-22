//! Fresh native disclosure admission for a saved original account and target.
//! No Git selection, provider request, runtime monitor adoption or row mutation.
use std::sync::{Arc, Weak};

use intent_core::repository_request::RepositoryPrivateOutput;
use intent_core::review_monitor::ReviewMonitorBinding;
use intent_core::{ExecutionScope, RepositoryProvider, Result};

use super::super::{authority, unavailable, Request};
use super::Recipient;
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    self as credentials, RepositoryCredentialError, RepositoryCredentialUse,
};
use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;

/// The whole response retains every required original account fence. A later
/// account replacement withholds the envelope, including already queued bytes.
pub(crate) struct Projection {
    recipient: Arc<Recipient>,
    admissions: Vec<Arc<RepositoryReadEligibility>>,
}
impl Projection {
    pub(super) fn new(
        recipient: Arc<Recipient>,
        admissions: Vec<Arc<RepositoryReadEligibility>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            recipient,
            admissions,
        })
    }
    pub(in crate::repository_native_wire) fn with_current(
        &self,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        self.recipient.with_current(&mut || {
            if self.admissions.is_empty() {
                action()
            } else {
                let required = self.admissions.iter().map(Arc::as_ref).collect::<Vec<_>>();
                let mut result = None;
                RepositoryReadEligibility::with_all_current_and_facts(&required, None, |_| {
                    result = Some(action());
                    Ok(())
                })
                .map_err(|_| unavailable())?;
                result.ok_or_else(unavailable)?
            }
        })
    }
    pub(super) fn history_inputs(
        &self,
    ) -> (
        Vec<super::RepositoryRetirement>,
        Vec<Arc<RepositoryReadEligibility>>,
    ) {
        (
            vec![
                self.recipient.connection.parent.clone(),
                self.recipient.receipt.lifetime.clone(),
                self.recipient.lifetime.retirement().clone(),
            ],
            self.admissions.clone(),
        )
    }

    pub(in crate::repository_native_wire) fn output(
        self: &Arc<Self>,
        frame: &str,
    ) -> Arc<dyn RepositoryPrivateOutput> {
        Arc::new(Output {
            projection: self.clone(),
            frame: frame.into(),
        })
    }
}
struct Output {
    projection: Arc<Projection>,
    frame: String,
}
impl RepositoryPrivateOutput for Output {
    fn frame(&self) -> &str {
        &self.frame
    }
    fn with_current(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.projection.with_current(action)
    }
}

struct ReadAuthority {
    request: Weak<Request>,
    recipient: Arc<Recipient>,
    expected: RepositoryAuthorityRequest,
}
struct Fence(Arc<ReadAuthority>);
impl RepositoryAuthorityFence for Fence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> credentials::Result<()> + Send),
    ) -> credentials::Result<()> {
        let mut result = None;
        self.0
            .recipient
            .with_current(&mut || {
                result = Some(action());
                Ok(())
            })
            .map_err(|_| RepositoryCredentialError::Retired)?;
        result.ok_or(RepositoryCredentialError::Retired)?
    }
}
impl RepositoryAuthority for Arc<ReadAuthority> {
    fn revalidate<'a>(
        &'a self,
        expected: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if expected != &self.expected {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            let request = self
                .request
                .upgrade()
                .ok_or(RepositoryCredentialError::Retired)?;
            if authority(&request, &self.recipient.workspace)
                .await
                .map_err(|_| RepositoryCredentialError::AuthorityDenied)?
                != self.recipient.authority
            {
                return Err(RepositoryCredentialError::Retired);
            }
            Ok(Box::new(Fence(self.clone())) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

pub(super) struct Admitted {
    pub(super) eligibility: Arc<RepositoryReadEligibility>,
    pub(super) binding: ReviewMonitorBinding,
}

pub(super) async fn acquire(
    request: &Arc<Request>,
    recipient: &Arc<Recipient>,
    row: &intent_store::QualifiedPrMonitor,
) -> Result<Admitted> {
    let intent_store::PersistedMonitorTarget::Resolved {
        target,
        provenance: intent_store::MonitorTargetProvenance::CapturedRequest,
    } = row.target()
    else {
        return Err(unavailable());
    };
    if target.repository.provider != RepositoryProvider::Gitlab
        || row.monitor().workspace_id != recipient.workspace
    {
        return Err(unavailable());
    }
    let envelope: serde_json::Value = serde_json::from_str(
        row.monitor()
            .last_snapshot
            .as_deref()
            .ok_or_else(unavailable)?,
    )
    .map_err(|_| unavailable())?;
    let original: ReviewMonitorBinding =
        serde_json::from_value(envelope["monitorBinding"].clone()).map_err(|_| unavailable())?;
    if original.resource != *target {
        return Err(unavailable());
    }
    acquire_binding(request, recipient, &original, false).await
}

/// Events cannot revive a saved connection generation as a new delivery.
/// Their original producer and the current recipient must admit the same live
/// account generation. The caller supplies only a typed private-bus binding.
pub(super) async fn acquire_event(
    request: &Arc<Request>,
    recipient: &Arc<Recipient>,
    original: &ReviewMonitorBinding,
) -> Result<Admitted> {
    acquire_binding(request, recipient, original, true).await
}

pub(super) async fn acquire_history(
    request: &Arc<Request>,
    recipient: &Arc<Recipient>,
    original: &ReviewMonitorBinding,
) -> Result<Admitted> {
    acquire_binding(request, recipient, original, false).await
}

async fn acquire_binding(
    request: &Arc<Request>,
    recipient: &Arc<Recipient>,
    original: &ReviewMonitorBinding,
    exact: bool,
) -> Result<Admitted> {
    let services = &request.connection.services;
    let target = &original.resource;
    if target.repository.provider != RepositoryProvider::Gitlab {
        return Err(unavailable());
    }
    let settled = services
        .gitlab_repository_settled_connection()
        .map_err(|_| unavailable())?;
    let selected = settled.selected();
    // Verified account identity must match the original writer. Connection and
    // daemon generations are freshly captured, never reconstructed from disk.
    if selected.binding.scope.account_id != original.connection.account_id
        || (exact
            && (selected.binding.scope != original.connection
                || original.scope.daemon_id != services.daemon_boot_id))
    {
        return Err(unavailable());
    }
    let expected = RepositoryAuthorityRequest {
        execution: ExecutionScope {
            daemon_id: services.daemon_boot_id.clone(),
            authority_scope_id: uuid::Uuid::new_v4().to_string(),
            authority_generation: recipient
                .authority
                .workspace
                .revision
                .ok_or_else(unavailable)?
                .get(),
        },
        target: target.repository.clone(),
        connection: selected.binding.scope.clone(),
        use_kind: RepositoryCredentialUse::NativeRead,
        allowed_transport: RepositoryCredentialTransport::GitlabApi(settled.descriptor().clone()),
    };
    let proof = Arc::new(ReadAuthority {
        request: Arc::downgrade(request),
        recipient: recipient.clone(),
        expected: expected.clone(),
    });
    let admission = services
        .repository_connection_directory()
        .admit(&selected.binding, expected.clone(), Arc::new(proof.clone()))
        .map_err(|_| unavailable())?;
    let eligibility = Arc::new(
        services
            .gitlab_repository_read_eligibility(&admission)
            .map_err(|_| unavailable())?,
    );
    // Admission DTOs alone do not authorize disclosure. Revalidate Store-backed
    // native authority and the actual original owner metadata together.
    proof
        .revalidate(&expected)
        .await
        .map_err(|_| unavailable())?
        .dispatch(&mut || eligibility.check())
        .map_err(|_| unavailable())?;
    Ok(Admitted {
        eligibility,
        binding: ReviewMonitorBinding {
            scope: expected.execution,
            connection: expected.connection,
            resource: target.clone(),
        },
    })
}
