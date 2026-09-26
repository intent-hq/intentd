//! Host authority storage. Each join/removal takes the `SQLite` write lock
//! before checking authority and keeps it until every related row commits.
//! Services must verify link secrets/proofs before calling, and invalidate
//! caches/egress after commit. No workspace grant is synthesized for members.

use intent_core::{
    now_iso, HostInvite, HostMember, HostMembershipState, HostRole, Principal, PrincipalId,
    PrincipalIdentity, WorkspaceId,
};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

use crate::principal_repo::{
    bind_identity, bind_principal, map_principal_row, INVITE_OPEN, PRINCIPAL_BY_IDENTITY,
    PRINCIPAL_COLUMNS, PRINCIPAL_UPSERT_SET,
};
use crate::{Error, Result, Store};

const HOST_INVITE_COLUMNS: &str = "id, secret_hash, secret, created_by_principal_id, \
    pin_identity_provider, pin_instance_host, pin_external_user_id, pin_login, \
    created_at, expires_at, redeemed_at, redeemed_by_principal_id, revoked_at, redemption_count";

// SQLite date arithmetic rounds fractional seconds. Include the rounded
// boundary and apply HostInvite::is_open_at to those candidates in Rust.
const HOST_INVITE_CANDIDATE: &str = "revoked_at IS NULL AND redeemed_at IS NULL \
    AND redemption_count = 0 AND julianday(expires_at) >= julianday(?)";

/// A roster and its revision read from one `SQLite` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostMembersSnapshot {
    pub members: Vec<HostMember>,
    pub revision: u64,
}

/// A verified proof may mint a credential. A returning bearer is validated
/// again in the transaction and reused without rotation or a replacement row.
#[derive(Debug, Clone, Copy)]
pub enum HostJoinCredential<'a> {
    Proof {
        token_hash: &'a str,
        authorization_generation: u64,
    },
    Existing {
        token_hash: &'a str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostInviteJoinOutcome {
    Joined {
        principal: Box<Principal>,
        membership_added: bool,
        revision: u64,
    },
    NotFound,
    Redeemed,
    Revoked,
    Expired,
    PinMismatch,
    OwnerSelfJoin,
    CredentialInvalid,
    AccessRevoked,
}

/// Durable part of a member removal. The service still owns queued-message
/// admission, cache invalidation, events and live connection closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostMemberRemoval {
    pub removed: bool,
    pub revision: u64,
    pub credentials: u64,
    pub workspaces: Vec<WorkspaceId>,
    pub revoked_invites: Vec<String>,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Result::map_err passes its owned database error"
)]
fn db_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("host membership storage failed: {error}"))
}

fn unsigned(row: &SqliteRow, column: &str) -> u64 {
    u64::try_from(row.get::<i64, _>(column)).unwrap_or(0)
}

async fn state_in_tx(conn: &mut SqliteConnection) -> Result<HostMembershipState> {
    let row = sqlx::query(
        "SELECT revision, member_count, authorization_generation FROM host_membership_state WHERE id = 1",
    )
    .fetch_one(conn)
    .await
    .map_err(db_error)?;
    Ok(HostMembershipState {
        revision: unsigned(&row, "revision"),
        member_count: unsigned(&row, "member_count"),
        authorization_generation: unsigned(&row, "authorization_generation"),
    })
}

impl Store {
    /// Read durable counters, also suitable for taking a proof challenge's
    /// authorization-generation snapshot before its identity is known.
    ///
    /// # Errors
    /// Returns `Internal` for a database failure.
    pub async fn host_membership_state(&self) -> Result<HostMembershipState> {
        state_in_tx(&mut *self.read_pool().acquire().await.map_err(db_error)?).await
    }

    /// Resolve host authority without inspecting any workspace membership.
    ///
    /// # Errors
    /// Returns `NotFound` for an unknown principal, `Internal` on DB failure.
    pub async fn get_host_role(&self, principal_id: &PrincipalId) -> Result<HostRole> {
        let row = sqlx::query(
            "SELECT p.is_primary, h.principal_id IS NOT NULL AS is_member \
             FROM principal p LEFT JOIN host_member h ON h.principal_id = p.id WHERE p.id = ?",
        )
        .bind(&principal_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(db_error)?
        .ok_or_else(|| Error::NotFound(format!("principal {principal_id}")))?;
        Ok(if row.get::<bool, _>("is_primary") {
            HostRole::Owner
        } else if row.get::<bool, _>("is_member") {
            HostRole::Member
        } else {
            HostRole::Guest
        })
    }

    /// Owner first, then members by grant time and ID. Profiles and revision
    /// are read in the same statement, including on a no-repository host.
    ///
    /// # Errors
    /// Returns `Internal` for a database failure.
    pub async fn list_host_members(&self) -> Result<HostMembersSnapshot> {
        let rows = sqlx::query(
            "SELECT p.id, p.is_primary, p.login, p.display_name, p.avatar_url, \
                    p.identity_provider, p.instance_host, p.external_user_id, \
                    CASE WHEN p.is_primary = 1 THEN p.created_at ELSE h.added_at END AS added_at, \
                    s.revision \
             FROM principal p LEFT JOIN host_member h ON h.principal_id = p.id \
             CROSS JOIN host_membership_state s \
             WHERE s.id = 1 AND (p.is_primary = 1 OR h.principal_id IS NOT NULL) \
             ORDER BY p.is_primary DESC, added_at, p.id",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(db_error)?;
        let revision = rows.first().map_or(0, |row| unsigned(row, "revision"));
        let members = rows
            .iter()
            .map(|row| HostMember {
                principal_id: PrincipalId(row.get("id")),
                host_role: if row.get::<bool, _>("is_primary") {
                    HostRole::Owner
                } else {
                    HostRole::Member
                },
                login: row.get("login"),
                display_name: row.get("display_name"),
                avatar_url: row.get("avatar_url"),
                identity: row
                    .get::<Option<String>, _>("identity_provider")
                    .map(|provider| PrincipalIdentity {
                        provider,
                        host: row.get("instance_host"),
                        external_user_id: row.get("external_user_id"),
                    }),
                added_at: row.get("added_at"),
            })
            .collect();
        Ok(HostMembersSnapshot { members, revision })
    }

    /// Insert a new pinned invite; the creator must still be the primary.
    /// No linked owner profile or repository credential is consulted.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an invalid invite/non-owner issuer, or
    /// `Internal` on database failure (including a duplicate invitation).
    pub async fn insert_host_invite(&self, invite: &HostInvite) -> Result<()> {
        invite.validate_new()?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let owner: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal WHERE id = ? AND is_primary = 1)",
        )
        .bind(&invite.created_by_principal_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        if !owner {
            return Err(Error::InvalidInput(
                "only the host owner may issue host invitations".into(),
            ));
        }
        let sql = format!("INSERT INTO host_invite ({HOST_INVITE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,0)");
        sqlx::query(&sql)
            .bind(&invite.id)
            .bind(&invite.secret_hash)
            .bind(&invite.secret)
            .bind(&invite.created_by_principal_id.0)
            .bind(&invite.pin_identity.provider)
            .bind(&invite.pin_identity.host)
            .bind(&invite.pin_identity.external_user_id)
            .bind(&invite.pin_login)
            .bind(&invite.created_at)
            .bind(&invite.expires_at)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)
    }

    /// Read only the host scope, retaining closed history.
    ///
    /// # Errors
    /// Returns `Internal` for a database failure.
    pub async fn get_host_invite(&self, id: &str) -> Result<Option<HostInvite>> {
        let sql = format!("SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE id = ?");
        Ok(sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(db_error)?
            .as_ref()
            .map(map_invite))
    }

    /// List open host invitations by creation time, then ID.
    ///
    /// # Errors
    /// Returns `Internal` for a database failure.
    pub async fn list_open_host_invites(&self) -> Result<Vec<HostInvite>> {
        self.list_open_host_invites_at(&now_iso()).await
    }

    pub(crate) async fn list_open_host_invites_at(&self, now: &str) -> Result<Vec<HostInvite>> {
        let sql = format!("SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE {HOST_INVITE_CANDIDATE} ORDER BY created_at, id");
        Ok(sqlx::query(&sql)
            .bind(now)
            .fetch_all(self.read_pool())
            .await
            .map_err(db_error)?
            .iter()
            .map(map_invite)
            .filter(|invite| invite.is_open_at(now))
            .collect())
    }

    /// Revoke only a usable host invitation. Unknown/wrong-scope IDs are
    /// `NotFound`; already closed invitations are an unchanged `false`.
    ///
    /// # Errors
    /// Returns `NotFound` for an unknown host invite, `Internal` on DB failure.
    pub async fn revoke_host_invite(&self, id: &str) -> Result<bool> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let sql = format!("SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE id = ?");
        let Some(row) = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?
        else {
            return Err(Error::NotFound(format!("host invite {id}")));
        };
        let now = now_iso();
        if !map_invite(&row).is_open_at(&now) {
            return Ok(false);
        }
        sqlx::query("UPDATE host_invite SET revoked_at = ? WHERE id = ?")
            .bind(&now)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(true)
    }

    /// Redeem one verified host invite, resolving the full stable identity
    /// and atomically stamping the invite, granting membership, and minting
    /// (proof) or rechecking/reusing (accept) a credential. All refusals write
    /// nothing, even profile fields. Concurrent single-use losers are Redeemed.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a missing identity/empty hash, `Internal`
    /// on database failure. A failed mint rolls back the entire join.
    pub async fn join_host_by_invite(
        &self,
        invite_id: &str,
        identity: &Principal,
        credential: HostJoinCredential<'_>,
    ) -> Result<HostInviteJoinOutcome> {
        let key = identity
            .identity_key()
            .ok_or_else(|| Error::InvalidInput("host join requires a verified identity".into()))?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let sql = format!("SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE id = ?");
        let Some(row) = sqlx::query(&sql)
            .bind(invite_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?
        else {
            return Ok(HostInviteJoinOutcome::NotFound);
        };
        let invite = map_invite(&row);
        if invite.revoked_at.is_some() {
            return Ok(HostInviteJoinOutcome::Revoked);
        }
        if invite.redeemed_at.is_some() {
            return Ok(HostInviteJoinOutcome::Redeemed);
        }
        let now = now_iso();
        if !invite.is_open_at(&now) {
            return Ok(HostInviteJoinOutcome::Expired);
        }
        if invite.pin_identity != key {
            return Ok(HostInviteJoinOutcome::PinMismatch);
        }
        let lookup =
            format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE {PRINCIPAL_BY_IDENTITY}");
        let existing = bind_identity(sqlx::query(&lookup), &key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?
            .as_ref()
            .map(map_principal_row);
        if existing.as_ref().is_some_and(|p| p.is_primary) || identity.is_primary {
            return Ok(HostInviteJoinOutcome::OwnerSelfJoin);
        }
        let state = state_in_tx(&mut tx).await?;
        let token_hash = match credential {
            HostJoinCredential::Proof {
                token_hash,
                authorization_generation,
            } => {
                let revoked: Option<i64> = if let Some(principal) = &existing {
                    sqlx::query_scalar(
                        "SELECT generation FROM principal_revocation WHERE principal_id = ?",
                    )
                    .bind(&principal.id.0)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db_error)?
                } else {
                    None
                };
                if authorization_generation > state.authorization_generation
                    || revoked.is_some_and(|generation| {
                        u64::try_from(generation).unwrap_or(u64::MAX) > authorization_generation
                    })
                {
                    return Ok(HostInviteJoinOutcome::AccessRevoked);
                }
                token_hash
            }
            HostJoinCredential::Existing { token_hash } => {
                let Some(principal) = &existing else {
                    return Ok(HostInviteJoinOutcome::CredentialInvalid);
                };
                let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM principal_credential WHERE token_hash = ? AND principal_id = ? AND revoked_at IS NULL)")
                    .bind(token_hash).bind(&principal.id.0).fetch_one(&mut *tx).await.map_err(db_error)?;
                if !active {
                    return Ok(HostInviteJoinOutcome::CredentialInvalid);
                }
                token_hash
            }
        };
        if token_hash.is_empty() {
            return Err(Error::InvalidInput("empty credential hash".into()));
        }
        let was_existing = existing.is_some();
        let mut principal = existing.unwrap_or_else(|| identity.clone());
        principal.set_identity(key);
        principal.login.clone_from(&identity.login);
        principal.display_name.clone_from(&identity.display_name);
        principal.avatar_url.clone_from(&identity.avatar_url);
        principal.updated_at.clone_from(&now);
        // A new identity may not overwrite an unrelated existing ID. Only a
        // principal resolved by its stable key can take the upsert path.
        let upsert = format!(
            "INSERT INTO principal ({PRINCIPAL_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?){}",
            if was_existing {
                format!(" ON CONFLICT(id) DO UPDATE SET {PRINCIPAL_UPSERT_SET}")
            } else {
                String::new()
            }
        );
        bind_principal(sqlx::query(&upsert), &principal)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        let membership_added = sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?,?) ON CONFLICT(principal_id) DO NOTHING")
            .bind(&principal.id.0).bind(&now).execute(&mut *tx).await.map_err(db_error)?.rows_affected() != 0;
        sqlx::query("UPDATE host_invite SET redeemed_at = ?, redeemed_by_principal_id = ?, redemption_count = 1 WHERE id = ?")
            .bind(&now).bind(&principal.id.0).bind(invite_id).execute(&mut *tx).await.map_err(db_error)?;
        if matches!(credential, HostJoinCredential::Proof { .. }) {
            sqlx::query("INSERT INTO principal_credential (token_hash, principal_id, created_at) VALUES (?,?,?)")
                .bind(token_hash).bind(&principal.id.0).bind(&now).execute(&mut *tx).await.map_err(db_error)?;
        }
        let revision = state_in_tx(&mut tx).await?.revision;
        tx.commit().await.map_err(db_error)?;
        Ok(HostInviteJoinOutcome::Joined {
            principal: Box::new(principal),
            membership_added,
            revision,
        })
    }

    /// Remove an active host member and their direct collaborator grants,
    /// revoke all their bearer credentials and every usable workspace invite
    /// they issued (including previously used reusable links). Closed history,
    /// other people and workspace ownership remain intact. A no-op neither
    /// revokes a workspace guest nor advances any clock.
    ///
    /// # Errors
    /// Returns `InvalidInput` for the primary, `Internal` for a DB failure.
    pub async fn remove_host_member(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<HostMemberRemoval> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let primary: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal WHERE id = ? AND is_primary = 1)",
        )
        .bind(&principal_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        if primary {
            return Err(Error::InvalidInput(
                "the host owner cannot be removed".into(),
            ));
        }
        let removed = sqlx::query("DELETE FROM host_member WHERE principal_id = ?")
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?
            .rows_affected()
            != 0;
        let mut result = HostMemberRemoval {
            removed,
            revision: state_in_tx(&mut tx).await?.revision,
            credentials: 0,
            workspaces: Vec::new(),
            revoked_invites: Vec::new(),
        };
        if removed {
            let now = now_iso();
            sqlx::query("UPDATE host_membership_state SET authorization_generation = authorization_generation + 1 WHERE id = 1")
                .execute(&mut *tx).await.map_err(db_error)?;
            sqlx::query("INSERT INTO principal_revocation (principal_id, generation) SELECT ?, authorization_generation FROM host_membership_state WHERE id = 1 ON CONFLICT(principal_id) DO UPDATE SET generation = excluded.generation")
                .bind(&principal_id.0).execute(&mut *tx).await.map_err(db_error)?;
            result.credentials = sqlx::query("UPDATE principal_credential SET revoked_at = ? WHERE principal_id = ? AND revoked_at IS NULL")
                .bind(&now).bind(&principal_id.0).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
            result.workspaces = sqlx::query_scalar::<_, String>("DELETE FROM workspace_member WHERE principal_id = ? AND role = 'collaborator' RETURNING workspace_id")
                .bind(&principal_id.0).fetch_all(&mut *tx).await.map_err(db_error)?.into_iter().map(WorkspaceId).collect();
            result.workspaces.sort_by(|a, b| a.0.cmp(&b.0));
            let revoke = format!("UPDATE workspace_invite AS i SET revoked_at = ? WHERE i.created_by_principal_id = ? AND {INVITE_OPEN} RETURNING id");
            result.revoked_invites = sqlx::query_scalar(&revoke)
                .bind(&now)
                .bind(&principal_id.0)
                .bind(&now)
                .fetch_all(&mut *tx)
                .await
                .map_err(db_error)?;
            result.revoked_invites.sort();
        }
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }
}

fn map_invite(row: &SqliteRow) -> HostInvite {
    HostInvite {
        id: row.get("id"),
        secret_hash: row.get("secret_hash"),
        secret: row.get("secret"),
        created_by_principal_id: PrincipalId(row.get("created_by_principal_id")),
        pin_identity: PrincipalIdentity {
            provider: row.get("pin_identity_provider"),
            host: row.get("pin_instance_host"),
            external_user_id: row.get("pin_external_user_id"),
        },
        pin_login: row.get("pin_login"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        redeemed_at: row.get("redeemed_at"),
        redeemed_by_principal_id: row
            .get::<Option<String>, _>("redeemed_by_principal_id")
            .map(PrincipalId),
        revoked_at: row.get("revoked_at"),
        redemption_count: unsigned(row, "redemption_count"),
    }
}
