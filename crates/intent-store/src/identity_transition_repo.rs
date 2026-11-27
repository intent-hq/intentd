//! Identity transitions checked in the write's own transaction (the
//! `identity_transition` gate of docs/ARCHITECTURE.md, "Owner work vs
//! request writes").
//!
//! The primary identity may change only while nothing depends on it (no
//! other principal and no open workspace invite), and an invite may be
//! minted only from the identity its issuer had when the service qualified
//! it. The service decides under its process-local `IdentityTransitionLock`,
//! but the decision reads rows another process can change before the write
//! lands. These writes repeat the checks that must still hold at commit:
//!
//! - **Postgres**: inside the write transaction, under the tenant writer
//!   lock, after locking the principal row (`FOR UPDATE`). A change committed
//!   by any process before the write is seen; the loser gets
//!   [`Error::Conflict`] (identity moved) or
//!   `Error::Invite(InviteErrorKind::IdentityLocked)` and writes nothing.
//! - **`SQLite`** (one process per database): the same checks as plain reads
//!   before the write. The service's process-local lock already orders them
//!   against every other writer of the file.

use intent_core::{Error, InviteErrorKind, Principal, PrincipalId, PrincipalIdentity, Result};
use intent_core::{HostInvite, WorkspaceInvite};

use crate::{InviteInsertOutcome, Store};

/// What [`Store::upsert_principal_checked`] requires of the stored row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityWriteCheck {
    /// The principal's identity as the caller read it before deciding
    /// (`None`: unlinked, or no row yet). A different stored identity fails
    /// with [`Error::Conflict`].
    pub expected: Option<PrincipalIdentity>,
    /// Refuse with `Error::Invite(InviteErrorKind::IdentityLocked)` when the
    /// primary identity is locked: another principal exists or a workspace
    /// invite is open.
    pub refuse_if_locked: bool,
}

/// The conflict a checked write returns when `principal_id`'s identity is no
/// longer the one the caller read.
pub(crate) fn identity_conflict(
    principal_id: &PrincipalId,
    current: Option<&PrincipalIdentity>,
) -> Error {
    Error::Conflict {
        current: serde_json::json!({
            "principalId": principal_id.0,
            "identity": current,
        }),
    }
}

pub(crate) fn identity_locked() -> Error {
    Error::Invite(InviteErrorKind::IdentityLocked)
}

impl Store {
    /// [`Store::upsert_principal`] that writes only while `check` holds (see
    /// the module docs).
    ///
    /// # Errors
    /// [`Error::Conflict`] when the stored identity is not `check.expected`;
    /// `Error::Invite(IdentityLocked)` when `check.refuse_if_locked` and the
    /// identity is locked; otherwise as [`Store::upsert_principal`].
    pub async fn upsert_principal_checked(
        &self,
        principal: &Principal,
        check: &IdentityWriteCheck,
    ) -> Result<()> {
        match &self.backend {
            crate::Backend::Sqlite(_) => {
                let current = match self.get_principal(&principal.id).await {
                    Ok(row) => row.identity_key(),
                    Err(Error::NotFound(_)) => None,
                    Err(e) => return Err(e),
                };
                if current != check.expected {
                    return Err(identity_conflict(&principal.id, current.as_ref()));
                }
                if check.refuse_if_locked
                    && (self.count_principals().await? > 1
                        || self.count_open_workspace_invites().await? > 0)
                {
                    return Err(identity_locked());
                }
                self.upsert_principal(principal).await
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(pg) => {
                crate::postgres::boxed_call(|| pg.upsert_principal_checked(principal, check)).await
            }
        }
    }

    /// [`Store::insert_workspace_invite_within_cap`] that inserts only while
    /// the issuer's identity is still `issuer_identity` (the identity it was
    /// qualified with).
    ///
    /// # Errors
    /// [`Error::Conflict`] when the issuer's identity changed; otherwise as
    /// [`Store::insert_workspace_invite_within_cap`].
    pub async fn insert_workspace_invite_within_cap_as(
        &self,
        invite: &WorkspaceInvite,
        max_guests: u32,
        issuer_identity: Option<&PrincipalIdentity>,
    ) -> Result<InviteInsertOutcome> {
        match &self.backend {
            crate::Backend::Sqlite(_) => {
                self.check_issuer_identity(&invite.created_by_principal_id, issuer_identity)
                    .await?;
                self.insert_workspace_invite_within_cap(invite, max_guests)
                    .await
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(pg) => {
                crate::postgres::boxed_call(|| {
                    pg.insert_workspace_invite_within_cap_as(invite, max_guests, issuer_identity)
                })
                .await
            }
        }
    }

    /// [`Store::insert_host_invite`] that inserts only while the issuer's
    /// identity is still `issuer_identity`.
    ///
    /// # Errors
    /// [`Error::Conflict`] when the issuer's identity changed; otherwise as
    /// [`Store::insert_host_invite`].
    pub async fn insert_host_invite_as(
        &self,
        invite: &HostInvite,
        issuer_identity: Option<&PrincipalIdentity>,
    ) -> Result<()> {
        match &self.backend {
            crate::Backend::Sqlite(_) => {
                self.check_issuer_identity(&invite.created_by_principal_id, issuer_identity)
                    .await?;
                self.insert_host_invite(invite).await
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(pg) => {
                crate::postgres::boxed_call(|| pg.insert_host_invite_as(invite, issuer_identity))
                    .await
            }
        }
    }

    async fn check_issuer_identity(
        &self,
        issuer: &PrincipalId,
        expected: Option<&PrincipalIdentity>,
    ) -> Result<()> {
        let current = self.get_principal(issuer).await?.identity_key();
        if current.as_ref() != expected {
            return Err(identity_conflict(issuer, current.as_ref()));
        }
        Ok(())
    }
}
