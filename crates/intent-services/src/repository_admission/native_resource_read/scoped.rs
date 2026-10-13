//! Original host-owner and workspace admission for the additive read surface.

use intent_core::caller::{Caller, WireCredential};
use intent_core::repository_resource_pages::{
    RepositoryReadContext as Context, RepositoryScopedBoundQuery, RepositoryScopedCapture,
    RepositoryScopedCaptureQuery, RepositoryScopedDetail, RepositoryScopedDetailQuery,
};
use intent_core::HostRole;
use intent_store::RepositoryHostAuthoritySnapshot;

use super::*;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Authority {
    Workspace(RepositoryAuthoritySnapshot),
    Host(RepositoryHostAuthoritySnapshot),
}

impl Authority {
    pub(super) fn generation(&self) -> Result<u64> {
        match self {
            Self::Workspace(facts) => facts
                .workspace
                .revision
                .map(|r| r.get())
                .ok_or_else(unavailable),
            Self::Host(facts) => facts
                .principal
                .revision
                .map(|r| r.get())
                .ok_or_else(unavailable),
        }
    }
}

pub(super) async fn authority(r: &Request) -> Result<Authority> {
    r.check()?;
    let c = &r.base.connection;
    c.services
        .require_host_execution("Repository resource read")
        .await
        .map_err(denied)?;
    match r.frame.context() {
        Context::Workspace { workspace_id } => super::super::authority(&r.base, &workspace_id)
            .await
            .map(Authority::Workspace)
            .map_err(denied),
        Context::Host {} => {
            c.entered().map_err(denied)?;
            // A host member's ability to browse checkout projects is not an
            // owner grant to enumerate private resources outside a workspace.
            let Caller::Wire {
                principal_id,
                host_role: HostRole::Owner,
            } = c.caller.caller()
            else {
                return Err(unavailable());
            };
            let hash = match c.caller.wire_credential() {
                Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
                _ => None,
            };
            let facts = c
                .services
                .store
                .repository_host_authority_snapshot(principal_id, hash)
                .await?;
            let owner = facts
                .principal
                .value
                .as_ref()
                .is_some_and(|p| &p.id == principal_id && p.is_primary)
                && facts
                    .primary_principal
                    .value
                    .as_ref()
                    .is_some_and(|p| &p.id == principal_id && p.is_primary);
            if !owner {
                return Err(unavailable());
            }
            match c.caller.wire_credential() {
                Some(WireCredential::Principal {
                    principal_id: original,
                    ..
                }) => {
                    let current = facts
                        .credential
                        .as_ref()
                        .and_then(|v| v.value.as_ref())
                        .ok_or_else(unavailable)?;
                    if original != principal_id
                        || &current.principal_id != original
                        || current.revoked
                    {
                        return Err(unavailable());
                    }
                }
                Some(WireCredential::Legacy { .. }) | None => {}
            }
            r.check()?;
            Ok(Authority::Host(facts))
        }
    }
}

pub(crate) fn capture(
    services: &Services,
    q: RepositoryScopedCaptureQuery,
) -> BoxFuture<'_, Result<RepositoryScopedCapture>> {
    entry(services, &Frame::CaptureScoped(q.clone()), move |r| {
        Box::pin(async move {
            let capture = tokio::time::timeout(ACQUIRE, capture_inner(&r))
                .await
                .map_err(denied)??;
            Ok(RepositoryScopedCapture {
                context: q.context,
                capture,
            })
        })
    })
}

pub(crate) fn detail(
    services: &Services,
    q: RepositoryScopedDetailQuery,
) -> BoxFuture<'_, Result<RepositoryScopedDetail>> {
    entry(services, &Frame::DetailScoped(q.clone()), move |r| {
        Box::pin(async move {
            let detail = detail_inner(&r, &q.read_lifetime_id, &q.target, q.refresh).await?;
            Ok(RepositoryScopedDetail {
                context: q.context,
                detail,
            })
        })
    })
}

pub(crate) fn release(
    services: &Services,
    q: RepositoryScopedBoundQuery,
) -> BoxFuture<'_, Result<Released>> {
    entry(services, &Frame::ReleaseScoped(q.clone()), move |r| {
        Box::pin(async move { release_inner(&r, &q.read_lifetime_id).await })
    })
}
