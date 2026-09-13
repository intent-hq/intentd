//! `principal.*` service logic and caller resolution (multiplayer w1).
//!
//! Every request reaches the service layer with a [`Caller`] bound by its
//! entry point (`intent_core::caller`). This module turns that binding into
//! a principal: a wire caller IS its principal; agents and hook runs act
//! for the daemon and resolve to the primary principal. An absent caller is
//! forbidden — never the primary user.
//!
//! The GitHub identity of the primary principal is attached lazily and off
//! the read path (stale-while-revalidate): a `principal.me` read for the
//! primary user serves the cached `principal` row and, at most once per
//! [`IDENTITY_REFRESH_INTERVAL`] per process, spawns a bounded background
//! refresh from `GET /user` when the source-control auth is configured. An
//! offline daemon keeps serving the cache.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    current_caller, now_iso, Caller, Error, Principal, PrincipalId, Result, Workspace,
};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::{pr_ops, Services};

/// Minimum spacing between two GitHub profile refreshes of the primary
/// principal within one daemon process.
const IDENTITY_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Bound on the network round trip; an offline daemon serves the cache.
const IDENTITY_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

/// Last successful/attempted identity refresh instant, shared across clones.
pub(crate) type IdentityRefreshState = Arc<tokio::sync::Mutex<Option<Instant>>>;

/// The forbidden error for a request with no bound caller.
pub(crate) fn no_caller() -> Error {
    Error::Internal("forbidden: request is not bound to a principal".to_string())
}

/// Resolve the current request's caller to a principal id. `None` when no
/// caller is bound.
pub(crate) async fn caller_principal_id(store: &Store) -> Result<Option<PrincipalId>> {
    match current_caller() {
        None => Ok(None),
        Some(Caller::Wire { principal_id, .. }) => Ok(Some(principal_id)),
        Some(Caller::Agent { .. } | Caller::Daemon) => {
            Ok(Some(store.get_primary_principal().await?.id))
        }
    }
}

/// `principal.me` wire shape.
pub(crate) fn principal_to_wire(p: &Principal, is_administrator: bool) -> Value {
    json!({
        "id": p.id,
        "login": p.login,
        "displayName": p.display_name,
        "avatarUrl": p.avatar_url,
        "isAdministrator": is_administrator,
    })
}

impl Services {
    /// `principal.me`: see [`intent_core::WorkspaceApi::principal_me`].
    pub(crate) async fn principal_me_op(&self) -> Result<Value> {
        let caller = current_caller().ok_or_else(no_caller)?;
        let principal = match &caller {
            Caller::Wire { principal_id, .. } => self.store.get_principal(principal_id).await?,
            Caller::Agent { .. } | Caller::Daemon => self.store.get_primary_principal().await?,
        };
        if principal.is_primary {
            self.spawn_primary_identity_refresh(principal.clone()).await;
        }
        let is_administrator = match caller {
            Caller::Wire {
                is_administrator, ..
            } => is_administrator,
            Caller::Agent { .. } | Caller::Daemon => principal.is_primary,
        };
        Ok(principal_to_wire(&principal, is_administrator))
    }

    /// Spawn a rate-limited, bounded background refresh of the primary
    /// principal's GitHub identity from `GET /user`. Detached: the read that
    /// triggered it never waits, and any failure (not configured, offline,
    /// timeout) leaves the cached row untouched.
    async fn spawn_primary_identity_refresh(&self, principal: Principal) {
        {
            let mut last = self.principal_identity_refreshed_at.lock().await;
            if last.is_some_and(|at| at.elapsed() < IDENTITY_REFRESH_INTERVAL) {
                return;
            }
            *last = Some(Instant::now());
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.refresh_primary_identity(principal).await;
        });
    }

    async fn refresh_primary_identity(&self, principal: Principal) {
        let fetched = tokio::time::timeout(IDENTITY_REFRESH_TIMEOUT, async {
            let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
            if !sc.check_auth().await.is_ok_and(|s| s.authenticated) {
                return Err(Error::Internal("github auth not configured".to_string()));
            }
            sc.get_user().await.map_err(pr_ops::map_sc_err)
        })
        .await;
        let user = match fetched {
            Ok(Ok(user)) => user,
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "principal.me: github identity refresh skipped");
                return;
            }
            Err(_) => {
                tracing::debug!("principal.me: github identity refresh timed out");
                return;
            }
        };
        let mut updated = principal.clone();
        updated.github_user_id = user.id.and_then(|id| i64::try_from(id).ok());
        updated.login = Some(user.login);
        updated.display_name = user.name;
        updated.avatar_url = user.avatar_url;
        if updated == principal {
            return;
        }
        updated.updated_at = now_iso();
        if let Err(e) = self.store.upsert_principal(&updated).await {
            tracing::warn!(error = %e, "principal.me: github identity persist failed");
        }
    }

    /// Attach the membership summary to one `workspace.get` row, relative
    /// to the current caller (one SQL query; a failure omits the fields).
    pub(crate) async fn attach_workspace_membership(&self, ws: &mut Workspace) {
        let viewer = caller_principal_id(&self.store).await.ok().flatten();
        match self
            .store
            .workspace_membership_summaries(viewer.as_ref(), Some(&ws.id))
            .await
        {
            Ok(mut map) => ws.membership = map.remove(&ws.id),
            Err(e) => tracing::debug!(error = %e, "workspace.get: membership summary failed"),
        }
    }

    /// Attach membership summaries to `workspace.list` rows in one query.
    pub(crate) async fn attach_workspace_memberships(&self, list: &mut [Workspace]) {
        let viewer = caller_principal_id(&self.store).await.ok().flatten();
        match self
            .store
            .workspace_membership_summaries(viewer.as_ref(), None)
            .await
        {
            Ok(mut map) => {
                for ws in list.iter_mut() {
                    ws.membership = map.remove(&ws.id);
                }
            }
            Err(e) => tracing::debug!(error = %e, "workspace.list: membership summary failed"),
        }
    }
}
