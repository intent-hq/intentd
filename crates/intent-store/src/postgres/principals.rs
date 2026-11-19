//! Principals, workspace membership, bearer credentials, workspace invites
//! and host membership (Postgres). Mirrors `principal_repo.rs`,
//! `host_membership_repo.rs` and `sharing_projection.rs`.
//!
//! As in `workspaces.rs`, the `SQLite` repository lifecycle brackets (and
//! the authority reads that only decide whether to open one) are left out:
//! a Postgres store never installs a lifecycle observer.
//!
//! Text ordering and timestamp comparisons use `COLLATE "C"` to match
//! `SQLite`'s byte order.

use std::collections::HashMap;

use intent_core::{
    now_iso, Error, HostInvite, HostMember, HostMembershipState, HostRole, Principal,
    PrincipalCredential, PrincipalId, PrincipalIdentity, Result, WorkspaceId, WorkspaceInvite,
    WorkspaceMember, WorkspaceMembership, WorkspaceRole, WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use sqlx::postgres::{PgConnection, PgRow, Postgres};
use sqlx::{Row, Transaction};

use super::{db, PgStore, WriteTx};
use crate::host_membership_repo::{
    HostInviteJoinOutcome, HostMemberRemoval, HostMembersSnapshot, OwnerQueuePermit,
};
use crate::principal_repo::{
    map_owner_violation, principal_identity_columns, ArchivedGuestSweep, CollaboratorAddOutcome,
    EffectiveWorkspaceMember, InviteInsertOutcome, InviteJoinOutcome, WorkspaceAuthorFallback,
    WorkspaceGuestCount, CREDENTIAL_COLUMNS, INVITE_COLUMNS, MEMBER_COLUMNS, PRINCIPAL_COLUMNS,
    PRINCIPAL_UPSERT_SET,
};
use crate::{enum_from_db, enum_to_db, HostJoinCredential};

/// [`PRINCIPAL_COLUMNS`] qualified by the alias `p`.
const P_COLUMNS: &str = "p.id, p.github_user_id, p.login, p.display_name, p.avatar_url, \
     p.is_primary, p.created_at, p.updated_at, p.identity_provider, p.instance_host, \
     p.external_user_id";

const HOST_INVITE_COLUMNS: &str = "id, secret_hash, secret, created_by_principal_id, \
    pin_identity_provider, pin_instance_host, pin_external_user_id, pin_login, \
    created_at, expires_at, redeemed_at, redeemed_by_principal_id, revoked_at, redemption_count";

/// `principal_repo::INVITE_OPEN` on a `workspace_invite` row aliased `i`,
/// with now bound as `$n`.
fn invite_open(n: usize) -> String {
    format!(
        "i.revoked_at IS NULL AND i.expires_at COLLATE \"C\" > ${n} \
         AND ((i.pin_identity_provider IS NULL AND i.pin_github_user_id IS NULL) \
              OR i.redeemed_at IS NULL)"
    )
}

/// The issuer check shared by invite inserts and joins: `$2` the issuer,
/// `$3` the workspace.
const ISSUER_AUTHORIZED: &str = "SELECT EXISTS(SELECT 1 FROM principal p WHERE p.tenant_id = $1 \
     AND p.id = $2 AND (p.is_primary = 1 \
     OR EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = p.id) \
     OR EXISTS(SELECT 1 FROM workspace_member m WHERE m.tenant_id = $1 \
         AND m.principal_id = p.id AND m.workspace_id = $3 AND m.role = 'owner')))";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn unsigned(row: &PgRow, name: &str) -> Result<u64> {
    Ok(u64::try_from(col::<i64>(row, name)?).unwrap_or(0))
}

fn identity_columns(
    row: &PgRow,
    provider: &str,
    host: &str,
    external_user_id: &str,
) -> Result<Option<PrincipalIdentity>> {
    let Some(provider) = col::<Option<String>>(row, provider)? else {
        return Ok(None);
    };
    Ok(Some(PrincipalIdentity {
        provider,
        host: col::<Option<String>>(row, host)?.unwrap_or_default(),
        external_user_id: col::<Option<String>>(row, external_user_id)?.unwrap_or_default(),
    }))
}

pub(super) fn principal(row: &PgRow) -> Result<Principal> {
    Ok(Principal {
        id: PrincipalId(col(row, "id")?),
        identity: identity_columns(
            row,
            "identity_provider",
            "instance_host",
            "external_user_id",
        )?,
        github_user_id: col(row, "github_user_id")?,
        login: col(row, "login")?,
        display_name: col(row, "display_name")?,
        avatar_url: col(row, "avatar_url")?,
        is_primary: col::<i64>(row, "is_primary")? != 0,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

fn member(row: &PgRow) -> Result<WorkspaceMember> {
    Ok(WorkspaceMember {
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        principal_id: PrincipalId(col(row, "principal_id")?),
        role: enum_from_db::<WorkspaceRole>(&col::<String>(row, "role")?)?,
        added_at: col(row, "added_at")?,
    })
}

fn credential(row: &PgRow) -> Result<PrincipalCredential> {
    Ok(PrincipalCredential {
        token_hash: col(row, "token_hash")?,
        principal_id: PrincipalId(col(row, "principal_id")?),
        created_at: col(row, "created_at")?,
        last_used_at: col(row, "last_used_at")?,
        revoked_at: col(row, "revoked_at")?,
    })
}

fn workspace_invite(row: &PgRow) -> Result<WorkspaceInvite> {
    Ok(WorkspaceInvite {
        id: col(row, "id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        secret_hash: col(row, "secret_hash")?,
        secret: col(row, "secret")?,
        created_by_principal_id: PrincipalId(col(row, "created_by_principal_id")?),
        pin_identity: identity_columns(
            row,
            "pin_identity_provider",
            "pin_instance_host",
            "pin_external_user_id",
        )?,
        pin_github_user_id: col(row, "pin_github_user_id")?,
        pin_login: col(row, "pin_login")?,
        created_at: col(row, "created_at")?,
        expires_at: col(row, "expires_at")?,
        redeemed_at: col(row, "redeemed_at")?,
        redeemed_by_principal_id: col::<Option<String>>(row, "redeemed_by_principal_id")?
            .map(PrincipalId),
        revoked_at: col(row, "revoked_at")?,
        redemption_count: unsigned(row, "redemption_count")?,
    })
}

fn host_invite(row: &PgRow) -> Result<HostInvite> {
    Ok(HostInvite {
        id: col(row, "id")?,
        secret_hash: col(row, "secret_hash")?,
        secret: col(row, "secret")?,
        created_by_principal_id: PrincipalId(col(row, "created_by_principal_id")?),
        pin_identity: PrincipalIdentity {
            provider: col(row, "pin_identity_provider")?,
            host: col(row, "pin_instance_host")?,
            external_user_id: col(row, "pin_external_user_id")?,
        },
        pin_login: col(row, "pin_login")?,
        created_at: col(row, "created_at")?,
        expires_at: col(row, "expires_at")?,
        redeemed_at: col(row, "redeemed_at")?,
        redeemed_by_principal_id: col::<Option<String>>(row, "redeemed_by_principal_id")?
            .map(PrincipalId),
        revoked_at: col(row, "revoked_at")?,
        redemption_count: unsigned(row, "redemption_count")?,
    })
}

fn host_role(is_primary: bool, is_member: bool) -> HostRole {
    if is_primary {
        HostRole::Owner
    } else if is_member {
        HostRole::Member
    } else {
        HostRole::Guest
    }
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// A sharing snapshot: read-only unless due invite reservations had to be
/// reconciled under the writer lock first.
enum SharingTx {
    Read(Transaction<'static, Postgres>),
    Write(WriteTx),
}

impl SharingTx {
    fn conn(&mut self) -> &mut PgConnection {
        match self {
            Self::Read(tx) => tx,
            Self::Write(tx) => tx,
        }
    }

    async fn commit(self) -> Result<()> {
        match self {
            Self::Read(tx) => tx.commit().await.map_err(db),
            Self::Write(tx) => tx.commit().await.map_err(db),
        }
    }
}

/// `sharing_projection::reconcile_invite_expiry`: drop the seat
/// reservations of invites in `workspace_ids` that expired by `at`.
async fn reconcile_invite_expiry(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_ids: &[String],
    at: &str,
) -> Result<()> {
    if workspace_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "DELETE FROM workspace_invite_seat WHERE tenant_id = $1 AND workspace_id = ANY($2) \
         AND expires_at COLLATE \"C\" <= $3",
    )
    .bind(tenant)
    .bind(workspace_ids)
    .bind(at)
    .execute(&mut **tx)
    .await
    .map_err(|e| Error::Internal(format!("reconcile invitation expiry failed: {e}")))?;
    Ok(())
}

/// `principal_repo::sync_workspace_owner`, inside the caller's write.
async fn sync_workspace_owner(
    tx: &mut WriteTx,
    tenant: &str,
    workspace_id: &WorkspaceId,
) -> Result<()> {
    sqlx::query(
        "UPDATE workspace SET owner_principal_id = COALESCE(\
            (SELECT m.principal_id FROM workspace_member m \
             WHERE m.tenant_id = $1 AND m.workspace_id = workspace.id AND m.role = 'owner' \
               AND m.principal_id = workspace.owner_principal_id), \
            (SELECT m.principal_id FROM workspace_member m \
             WHERE m.tenant_id = $1 AND m.workspace_id = workspace.id AND m.role = 'owner' \
             ORDER BY m.added_at COLLATE \"C\", m.principal_id COLLATE \"C\" LIMIT 1)) \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .execute(&mut **tx)
    .await
    .map_err(|e| Error::Internal(format!("sync workspace owner failed: {e}")))?;
    Ok(())
}

async fn workspace_archived(
    conn: &mut PgConnection,
    tenant: &str,
    workspace_id: &WorkspaceId,
) -> Result<Option<bool>> {
    let archived: Option<i64> =
        sqlx::query_scalar("SELECT archived FROM workspace WHERE tenant_id = $1 AND id = $2")
            .bind(tenant)
            .bind(&workspace_id.0)
            .fetch_optional(conn)
            .await
            .map_err(db)?;
    Ok(archived.map(|a| a != 0))
}

async fn member_role(
    conn: &mut PgConnection,
    tenant: &str,
    workspace_id: &WorkspaceId,
    principal_id: &PrincipalId,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT role FROM workspace_member WHERE tenant_id = $1 AND workspace_id = $2 \
         AND principal_id = $3",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .bind(&principal_id.0)
    .fetch_optional(conn)
    .await
    .map_err(db)
}

async fn is_host_member(
    conn: &mut PgConnection,
    tenant: &str,
    principal_id: &PrincipalId,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM host_member WHERE tenant_id = $1 AND principal_id = $2)",
    )
    .bind(tenant)
    .bind(&principal_id.0)
    .fetch_one(conn)
    .await
    .map_err(db)
}

async fn is_primary(conn: &mut PgConnection, tenant: &str, id: &PrincipalId) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM principal WHERE tenant_id = $1 AND id = $2 \
         AND is_primary = 1)",
    )
    .bind(tenant)
    .bind(&id.0)
    .fetch_one(conn)
    .await
    .map_err(db)
}

async fn principal_by_identity(
    conn: &mut PgConnection,
    tenant: &str,
    identity: &PrincipalIdentity,
) -> Result<Option<Principal>> {
    let sql = format!(
        "SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE tenant_id = $1 \
         AND identity_provider = $2 AND instance_host = $3 AND external_user_id = $4"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(identity.provider.as_str())
        .bind(identity.host.as_str())
        .bind(identity.external_user_id.as_str())
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .as_ref()
        .map(principal)
        .transpose()
}

async fn active_credential_of(
    conn: &mut PgConnection,
    tenant: &str,
    token_hash: &str,
    principal_id: &PrincipalId,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM principal_credential WHERE tenant_id = $1 \
         AND token_hash = $2 AND principal_id = $3 AND revoked_at IS NULL)",
    )
    .bind(tenant)
    .bind(token_hash)
    .bind(&principal_id.0)
    .fetch_one(conn)
    .await
    .map_err(db)
}

async fn revoked_generation(
    conn: &mut PgConnection,
    tenant: &str,
    principal_id: &PrincipalId,
) -> Result<Option<i64>> {
    sqlx::query_scalar(
        "SELECT generation FROM principal_revocation WHERE tenant_id = $1 AND principal_id = $2",
    )
    .bind(tenant)
    .bind(&principal_id.0)
    .fetch_optional(conn)
    .await
    .map_err(db)
}

async fn host_state(conn: &mut PgConnection, tenant: &str) -> Result<HostMembershipState> {
    let row = sqlx::query(
        "SELECT revision, member_count, authorization_generation FROM host_membership_state \
         WHERE tenant_id = $1 AND id = 1",
    )
    .bind(tenant)
    .fetch_one(conn)
    .await
    .map_err(db)?;
    Ok(HostMembershipState {
        revision: unsigned(&row, "revision")?,
        member_count: unsigned(&row, "member_count")?,
        authorization_generation: unsigned(&row, "authorization_generation")?,
    })
}

/// Committed guest seats (collaborators plus open invites) of a workspace.
async fn committed_seats(
    conn: &mut PgConnection,
    tenant: &str,
    workspace_id: &WorkspaceId,
) -> Result<u64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT guest_count + open_invite_count FROM workspace_sharing_summary \
         WHERE tenant_id = $1 AND workspace_id = $2",
    )
    .bind(tenant)
    .bind(&workspace_id.0)
    .fetch_one(conn)
    .await
    .map_err(db)?;
    Ok(u64::try_from(n).unwrap_or(u64::MAX))
}

/// Insert (`was_existing = false`) or upsert a principal row in the
/// caller's write. A new identity may not overwrite an unrelated id.
async fn write_principal(
    tx: &mut WriteTx,
    tenant: &str,
    p: &Principal,
    was_existing: bool,
) -> Result<()> {
    let conflict = if was_existing {
        format!(" ON CONFLICT (tenant_id, id) DO UPDATE SET {PRINCIPAL_UPSERT_SET}")
    } else {
        String::new()
    };
    let sql = format!(
        "INSERT INTO principal (tenant_id, {PRINCIPAL_COLUMNS}) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12){conflict}"
    );
    let (identity, github_user_id) = principal_identity_columns(p);
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(&p.id.0)
        .bind(github_user_id)
        .bind(&p.login)
        .bind(&p.display_name)
        .bind(&p.avatar_url)
        .bind(i64::from(p.is_primary))
        .bind(&p.created_at)
        .bind(&p.updated_at)
        .bind(identity.as_ref().map(|i| i.provider.clone()))
        .bind(identity.as_ref().map(|i| i.host.clone()))
        .bind(identity.map(|i| i.external_user_id))
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("write principal failed: {e}")))?;
    Ok(())
}

async fn insert_credential(
    tx: &mut WriteTx,
    tenant: &str,
    token_hash: &str,
    principal_id: &PrincipalId,
    now: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO principal_credential (tenant_id, token_hash, principal_id, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant)
    .bind(token_hash)
    .bind(&principal_id.0)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(|e| Error::Internal(format!("insert principal credential failed: {e}")))?;
    Ok(())
}

impl PgStore {
    /// `SqliteStore::sharing_snapshot`: one read-only snapshot of `ids`'
    /// sharing counters (so the due-seat probe and the count agree),
    /// restarted under the writer lock to drop due reservations.
    async fn sharing_snapshot(&self, ids: &[String]) -> Result<SharingTx> {
        let mut tx = self.begin_read_snapshot().await?;
        let due: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM workspace_invite_seat WHERE tenant_id = $1 AND workspace_id = ANY($2) \
             AND expires_at COLLATE \"C\" <= $3 LIMIT 1",
        )
        .bind(self.tenant())
        .bind(ids)
        .bind(now_iso())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if due.is_none() {
            return Ok(SharingTx::Read(tx));
        }
        tx.rollback().await.map_err(db)?;
        let mut tx = self.begin_write().await?;
        reconcile_invite_expiry(&mut tx, self.tenant(), ids, &now_iso()).await?;
        Ok(SharingTx::Write(tx))
    }

    async fn fetch_principals(&self, sql: &str) -> Result<Vec<Principal>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .bind(self.tenant())
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(principal).collect()
    }

    pub async fn get_principal(&self, id: &PrincipalId) -> Result<Principal> {
        let sql =
            format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE tenant_id = $1 AND id = $2");
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref()
            .map(principal)
            .transpose()?
            .ok_or_else(|| Error::NotFound(format!("principal {id}")))
    }

    pub async fn get_principals(&self, ids: &[PrincipalId]) -> Result<Vec<Principal>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<&str> = ids.iter().map(|p| p.0.as_str()).collect();
        let sql = format!(
            "SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE tenant_id = $1 AND id = ANY($2)"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(principal).collect()
    }

    pub async fn get_device_principals(
        &self,
        ids: &[PrincipalId],
        viewer: Option<&PrincipalId>,
    ) -> Result<Vec<(Principal, HostRole)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<&str> = ids.iter().map(|p| p.0.as_str()).collect();
        let mut sql = format!(
            "SELECT {P_COLUMNS}, EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 \
                 AND h.principal_id = p.id) AS is_member \
             FROM principal p WHERE p.tenant_id = $1 AND p.id = ANY($2)"
        );
        if viewer.is_some() {
            sql.push_str(
                " AND (p.id = $3 OR EXISTS(SELECT 1 FROM principal v WHERE v.tenant_id = $1 \
                 AND v.id = $3 AND (v.is_primary = 1 OR EXISTS(SELECT 1 FROM host_member h \
                 WHERE h.tenant_id = $1 AND h.principal_id = v.id))))",
            );
        }
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&ids);
        if let Some(viewer) = viewer {
            query = query.bind(viewer.as_str());
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                let person = principal(row)?;
                let role = host_role(person.is_primary, col(row, "is_member")?);
                Ok((person, role))
            })
            .collect()
    }

    pub async fn get_primary_principal(&self) -> Result<Principal> {
        self.fetch_principals(&format!(
            "SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE tenant_id = $1 AND is_primary = 1"
        ))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Internal("primary principal missing".to_string()))
    }

    pub async fn find_principal_by_identity(
        &self,
        identity: &PrincipalIdentity,
    ) -> Result<Option<Principal>> {
        let mut tx = self.begin().await?;
        let found = principal_by_identity(&mut tx, self.tenant(), identity).await?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn find_principal_by_github_user_id(
        &self,
        github_user_id: i64,
    ) -> Result<Option<Principal>> {
        self.find_principal_by_identity(&PrincipalIdentity::github(github_user_id))
            .await
    }

    pub async fn list_principals(&self) -> Result<Vec<Principal>> {
        self.fetch_principals(&format!(
            "SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE tenant_id = $1 \
             ORDER BY is_primary DESC, created_at COLLATE \"C\", id COLLATE \"C\""
        ))
        .await
    }

    pub async fn list_credentialed_guest_principals(&self) -> Result<Vec<Principal>> {
        self.fetch_principals(&format!(
            "SELECT {P_COLUMNS} FROM principal p \
             WHERE p.tenant_id = $1 AND p.is_primary = 0 AND EXISTS (\
                 SELECT 1 FROM principal_credential c \
                 WHERE c.tenant_id = $1 AND c.principal_id = p.id AND c.revoked_at IS NULL) \
             ORDER BY p.created_at COLLATE \"C\", p.id COLLATE \"C\""
        ))
        .await
    }

    pub async fn list_sharing_principals(&self) -> Result<Vec<(Principal, HostRole)>> {
        let sql = format!(
            "SELECT {P_COLUMNS}, h.principal_id IS NOT NULL AS is_host_member \
             FROM principal p LEFT JOIN host_member h ON h.tenant_id = $1 AND h.principal_id = p.id \
             WHERE p.tenant_id = $1 AND p.is_primary = 0 AND EXISTS (SELECT 1 \
                 FROM principal_credential c WHERE c.tenant_id = $1 \
                 AND c.principal_id = p.id AND c.revoked_at IS NULL) \
             ORDER BY p.created_at COLLATE \"C\", p.id COLLATE \"C\""
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|r| Ok((principal(r)?, host_role(false, col(r, "is_host_member")?))))
            .collect()
    }

    pub async fn upsert_principal(&self, p: &Principal) -> Result<()> {
        let mut tx = self.begin_write().await?;
        write_principal(&mut tx, self.tenant(), p, true).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_workspace_owner_principal_id(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<PrincipalId>> {
        Ok(self
            .get_workspace_author_fallback(workspace_id)
            .await?
            .and_then(|f| f.owner_principal_id))
    }

    pub async fn get_workspace_author_fallback(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceAuthorFallback>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT legacy_author_principal_id, owner_principal_id FROM workspace \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.map(|r| {
            Ok(WorkspaceAuthorFallback {
                legacy_author_principal_id: col::<Option<String>>(
                    &r,
                    "legacy_author_principal_id",
                )?
                .map(PrincipalId),
                owner_principal_id: col::<Option<String>>(&r, "owner_principal_id")?
                    .map(PrincipalId),
            })
        })
        .transpose()
    }

    pub async fn set_workspace_legacy_author_principal_id(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: Option<&PrincipalId>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE workspace SET legacy_author_principal_id = $3 WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(principal_id.map(|p| p.0.as_str()))
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if updated == 0 {
            return Err(Error::NotFound(format!("workspace {workspace_id}")));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn workspace_membership_summaries(
        &self,
        viewer: Option<&PrincipalId>,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, WorkspaceMembership>> {
        if workspace_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids: Vec<String> = workspace_ids.iter().map(|w| w.0.clone()).collect();
        let mut tx = self.sharing_snapshot(&ids).await?;
        let rows = sqlx::query(
            "WITH viewer AS (SELECT p.tenant_id, p.id, p.is_primary, \
                 h.principal_id IS NOT NULL AS is_host_member \
                 FROM principal p LEFT JOIN host_member h \
                 ON h.tenant_id = $1 AND h.principal_id = p.id \
                 WHERE p.tenant_id = $1 AND p.id = $2) \
             SELECT w.id AS workspace_id, w.owner_principal_id, \
                COALESCE(v.is_primary = 1 OR CASE WHEN v.is_host_member THEN w.id <> $3 \
                    ELSE m.role = 'owner' END, false) AS can_manage, \
                CASE WHEN w.id = $3 THEN s.direct_count \
                    ELSE s.non_host_count + h.member_count END AS member_count, \
                s.open_invite_count, \
                COALESCE(m.role, CASE WHEN v.is_host_member AND w.id <> $3 \
                    THEN 'collaborator' END) AS my_role \
             FROM workspace w JOIN workspace_sharing_summary s \
                 ON s.tenant_id = $1 AND s.workspace_id = w.id \
             CROSS JOIN host_membership_state h LEFT JOIN viewer v ON v.tenant_id = $1 \
             LEFT JOIN workspace_member m \
                 ON m.tenant_id = $1 AND m.workspace_id = w.id AND m.principal_id = v.id \
             WHERE w.tenant_id = $1 AND h.tenant_id = $1 AND h.id = 1 AND w.id = ANY($4)",
        )
        .bind(self.tenant())
        .bind(viewer.map(|p| p.0.as_str()))
        .bind(CHIEF_WORKSPACE_ID)
        .bind(&ids)
        .fetch_all(tx.conn())
        .await
        .map_err(|e| Error::Internal(format!("workspace membership summaries failed: {e}")))?;
        tx.commit().await?;
        rows.iter()
            .map(|r| {
                let my_role = col::<Option<String>>(r, "my_role")?
                    .map(|role| enum_from_db::<WorkspaceRole>(&role))
                    .transpose()?;
                Ok((
                    WorkspaceId(col(r, "workspace_id")?),
                    WorkspaceMembership {
                        can_manage: col(r, "can_manage")?,
                        owner_principal_id: col::<Option<String>>(r, "owner_principal_id")?
                            .map(PrincipalId),
                        my_role,
                        member_count: unsigned(r, "member_count")?,
                        open_invite_count: unsigned(r, "open_invite_count")?,
                    },
                ))
            })
            .collect()
    }

    pub async fn list_workspace_members(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceMember>> {
        let sql = format!(
            "SELECT {MEMBER_COLUMNS} FROM workspace_member WHERE tenant_id = $1 \
             AND workspace_id = $2 ORDER BY CASE role WHEN 'owner' THEN 0 ELSE 1 END, \
             added_at COLLATE \"C\", principal_id COLLATE \"C\""
        );
        self.fetch_members(&sql, &workspace_id.0).await
    }

    async fn fetch_members(&self, sql: &str, key: &str) -> Result<Vec<WorkspaceMember>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .bind(self.tenant())
            .bind(key)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(member).collect()
    }

    pub async fn list_effective_workspace_members(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<EffectiveWorkspaceMember>> {
        let added = "CASE WHEN h.principal_id IS NOT NULL AND w.id <> $3 THEN h.added_at \
                     ELSE m.added_at END";
        let sql = format!(
            "SELECT {P_COLUMNS}, m.role AS direct_role, \
                 h.principal_id IS NOT NULL AS is_host_member, {added} AS member_added_at \
             FROM principal p JOIN workspace w ON w.tenant_id = $1 AND w.id = $2 \
             LEFT JOIN workspace_member m \
                 ON m.tenant_id = $1 AND m.workspace_id = w.id AND m.principal_id = p.id \
             LEFT JOIN host_member h ON h.tenant_id = $1 AND h.principal_id = p.id \
             WHERE p.tenant_id = $1 \
                 AND (m.principal_id IS NOT NULL OR (h.principal_id IS NOT NULL AND w.id <> $3)) \
             ORDER BY p.is_primary DESC, ({added}) COLLATE \"C\", p.id COLLATE \"C\""
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(CHIEF_WORKSPACE_ID)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("effective workspace roster failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|r| {
                let principal = principal(r)?;
                let host_member: bool = col(r, "is_host_member")?;
                Ok(EffectiveWorkspaceMember {
                    role: if principal.is_primary {
                        WorkspaceRole::Owner
                    } else if host_member {
                        WorkspaceRole::Collaborator
                    } else {
                        enum_from_db(&col::<String>(r, "direct_role")?)?
                    },
                    host_role: host_role(principal.is_primary, host_member),
                    principal,
                    added_at: col(r, "member_added_at")?,
                })
            })
            .collect()
    }

    pub async fn effective_principal_workspace_ids(
        &self,
        principal: &PrincipalId,
    ) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT u.id FROM (SELECT workspace_id AS id FROM workspace_member \
                 WHERE tenant_id = $1 AND principal_id = $2 \
             UNION SELECT w.id FROM workspace w WHERE w.tenant_id = $1 AND w.id <> $3 \
             AND substr(w.id, 1, 9) <> '__home__:' \
             AND EXISTS(SELECT 1 FROM host_member h WHERE h.tenant_id = $1 AND h.principal_id = $2)) u \
             ORDER BY u.id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(principal.as_str())
        .bind(CHIEF_WORKSPACE_ID)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn list_principal_memberships(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<Vec<WorkspaceMember>> {
        let sql = format!(
            "SELECT {MEMBER_COLUMNS} FROM workspace_member WHERE tenant_id = $1 \
             AND principal_id = $2 ORDER BY added_at COLLATE \"C\", workspace_id COLLATE \"C\""
        );
        self.fetch_members(&sql, &principal_id.0).await
    }

    pub async fn has_workspace_membership(&self, principal_id: &PrincipalId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let found = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workspace_member WHERE tenant_id = $1 \
             AND principal_id = $2)",
        )
        .bind(self.tenant())
        .bind(&principal_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }

    pub async fn get_workspace_member_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<Option<WorkspaceRole>> {
        let mut tx = self.begin().await?;
        let role = member_role(&mut tx, self.tenant(), workspace_id, principal_id).await?;
        tx.commit().await.map_err(db)?;
        role.map(|r| enum_from_db::<WorkspaceRole>(&r)).transpose()
    }

    pub async fn add_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let sql = format!(
            "INSERT INTO workspace_member (tenant_id, {MEMBER_COLUMNS}) VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, workspace_id, principal_id) DO NOTHING"
        );
        let inserted = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .bind(role.as_str())
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(|e| map_owner_violation(&e, workspace_id, "add workspace member"))?
            .rows_affected();
        sync_workspace_owner(&mut tx, self.tenant(), workspace_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(inserted > 0)
    }

    pub async fn add_workspace_collaborator_within_cap(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        max_guests: u32,
    ) -> Result<CollaboratorAddOutcome> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if workspace_id.as_str() != CHIEF_WORKSPACE_ID
            && is_host_member(&mut tx, tenant, principal_id).await?
        {
            return Ok(CollaboratorAddOutcome::AlreadyMember);
        }
        if workspace_archived(&mut tx, tenant, workspace_id).await? == Some(true) {
            return Ok(CollaboratorAddOutcome::WorkspaceArchived);
        }
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal_credential WHERE tenant_id = $1 \
             AND principal_id = $2 AND revoked_at IS NULL)",
        )
        .bind(tenant)
        .bind(&principal_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !active {
            return Ok(CollaboratorAddOutcome::NoActiveCredential);
        }
        if member_role(&mut tx, tenant, workspace_id, principal_id)
            .await?
            .is_some()
        {
            return Ok(CollaboratorAddOutcome::AlreadyMember);
        }
        reconcile_invite_expiry(
            &mut tx,
            tenant,
            std::slice::from_ref(&workspace_id.0),
            &now_iso(),
        )
        .await?;
        if committed_seats(&mut tx, tenant, workspace_id).await? >= u64::from(max_guests) {
            return Ok(CollaboratorAddOutcome::WorkspaceFull);
        }
        let sql = format!(
            "INSERT INTO workspace_member (tenant_id, {MEMBER_COLUMNS}) VALUES ($1, $2, $3, $4, $5)"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(tenant)
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .bind(WorkspaceRole::Collaborator.as_str())
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(|e| map_owner_violation(&e, workspace_id, "capped member add"))?;
        tx.commit().await.map_err(db)?;
        Ok(CollaboratorAddOutcome::Added)
    }

    pub async fn set_workspace_member_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE workspace_member SET role = $4 WHERE tenant_id = $1 AND workspace_id = $2 \
             AND principal_id = $3",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&principal_id.0)
        .bind(role.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|e| map_owner_violation(&e, workspace_id, "set workspace member role"))?
        .rows_affected();
        if updated == 0 {
            return Err(Error::NotFound(format!(
                "principal {principal_id} is not a member of workspace {workspace_id}"
            )));
        }
        sync_workspace_owner(&mut tx, self.tenant(), workspace_id).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn set_workspace_guest_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
        actor: Option<&PrincipalId>,
    ) -> Result<bool> {
        if role == WorkspaceRole::Owner {
            return Err(Error::InvalidParams("the owner role is immutable".into()));
        }
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if let Some(actor) = actor {
            let owner = member_role(&mut tx, tenant, workspace_id, actor).await?;
            if !(is_primary(&mut tx, tenant, actor).await? || owner.as_deref() == Some("owner")) {
                return Err(Error::Forbidden(
                    "workspace.members.setRole requires the workspace owner".into(),
                ));
            }
        }
        let previous = member_role(&mut tx, tenant, workspace_id, principal_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("workspace member {principal_id}")))?;
        if previous == "owner" {
            return Err(Error::InvalidParams("the owner role is immutable".into()));
        }
        if is_host_member(&mut tx, tenant, principal_id).await?
            || is_primary(&mut tx, tenant, principal_id).await?
        {
            return Err(Error::HostMembershipRequired);
        }
        let changed = previous != role.as_str();
        if changed {
            sqlx::query(
                "UPDATE workspace_member SET role = $4 WHERE tenant_id = $1 \
                 AND workspace_id = $2 AND principal_id = $3",
            )
            .bind(tenant)
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .bind(role.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(changed)
    }

    async fn delete_member(
        tx: &mut WriteTx,
        tenant: &str,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let deleted = sqlx::query(
            "DELETE FROM workspace_member WHERE tenant_id = $1 AND workspace_id = $2 \
             AND principal_id = $3",
        )
        .bind(tenant)
        .bind(&workspace_id.0)
        .bind(&principal_id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(deleted > 0)
    }

    pub async fn remove_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let removed =
            Self::delete_member(&mut tx, self.tenant(), workspace_id, principal_id).await?;
        sync_workspace_owner(&mut tx, self.tenant(), workspace_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }

    pub async fn remove_workspace_guest(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if is_host_member(&mut tx, tenant, principal_id).await? {
            return Err(Error::HostMembershipRequired);
        }
        if member_role(&mut tx, tenant, workspace_id, principal_id)
            .await?
            .as_deref()
            == Some("owner")
        {
            return Err(Error::InvalidParams(
                "the workspace owner cannot leave or be removed".into(),
            ));
        }
        let removed = Self::delete_member(&mut tx, tenant, workspace_id, principal_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }

    pub async fn insert_principal_credential(
        &self,
        principal_id: &PrincipalId,
        token_hash: &str,
    ) -> Result<PrincipalCredential> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        insert_credential(&mut tx, self.tenant(), token_hash, principal_id, &now).await?;
        tx.commit().await.map_err(db)?;
        Ok(PrincipalCredential {
            token_hash: token_hash.to_string(),
            principal_id: principal_id.clone(),
            created_at: now,
            last_used_at: None,
            revoked_at: None,
        })
    }

    pub async fn lookup_principal_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<PrincipalCredential>> {
        let sql = format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM principal_credential WHERE tenant_id = $1 \
             AND token_hash = $2"
        );
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(token_hash)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(credential).transpose()
    }

    pub async fn list_principal_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<Vec<PrincipalCredential>> {
        let sql = format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM principal_credential WHERE tenant_id = $1 \
             AND principal_id = $2 ORDER BY created_at COLLATE \"C\" DESC, token_hash COLLATE \"C\""
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&principal_id.0)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(credential).collect()
    }

    pub async fn touch_principal_credential(&self, token_hash: &str) -> Result<bool> {
        Ok(self
            .resolve_active_principal_credential(token_hash)
            .await?
            .is_some())
    }

    pub async fn resolve_active_principal_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<PrincipalId>> {
        let mut tx = self.begin_write().await?;
        let id: Option<String> = sqlx::query_scalar(
            "UPDATE principal_credential SET last_used_at = $3 \
             WHERE tenant_id = $1 AND token_hash = $2 AND revoked_at IS NULL \
             RETURNING principal_id",
        )
        .bind(self.tenant())
        .bind(token_hash)
        .bind(now_iso())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(id.map(PrincipalId))
    }

    pub async fn revoke_principal_credential(&self, token_hash: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let revoked = sqlx::query(
            "UPDATE principal_credential SET revoked_at = $3 \
             WHERE tenant_id = $1 AND token_hash = $2 AND revoked_at IS NULL",
        )
        .bind(self.tenant())
        .bind(token_hash)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(revoked > 0)
    }

    /// `SqliteStore::owner_queue_permit` under the tenant writer lock, which
    /// every credential revoke takes too.
    pub async fn owner_queue_permit(
        &self,
        principal_id: &PrincipalId,
        token_hash: Option<&str>,
    ) -> Result<Option<OwnerQueuePermit>> {
        let mut tx = self.begin_write().await?;
        let authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal p WHERE p.tenant_id = $1 AND p.id = $2 \
             AND p.is_primary = 1 AND ($3::text IS NULL OR EXISTS(SELECT 1 \
             FROM principal_credential c WHERE c.tenant_id = $1 AND c.principal_id = p.id \
             AND c.token_hash = $3 AND c.revoked_at IS NULL)))",
        )
        .bind(self.tenant())
        .bind(&principal_id.0)
        .bind(token_hash)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        Ok(authorized.then(|| OwnerQueuePermit::postgres(tx)))
    }

    pub async fn revoke_all_principal_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let revoked =
            revoke_credentials_of(&mut tx, self.tenant(), principal_id, &now_iso()).await?;
        tx.commit().await.map_err(db)?;
        Ok(revoked)
    }

    pub async fn count_principals(&self) -> Result<u64> {
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM principal WHERE tenant_id = $1")
            .bind(self.tenant())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    pub async fn count_open_workspace_invites(&self) -> Result<u64> {
        let sql = format!(
            "SELECT COUNT(*) FROM workspace_invite i WHERE i.tenant_id = $1 AND {}",
            invite_open(2)
        );
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(now_iso())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    pub async fn count_workspace_guests(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceGuestCount> {
        let mut tx = self
            .sharing_snapshot(std::slice::from_ref(&workspace_id.0))
            .await?;
        let row = sqlx::query(
            "SELECT guest_count, open_invite_count FROM workspace_sharing_summary \
             WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_optional(tx.conn())
        .await
        .map_err(db)?;
        tx.commit().await?;
        let Some(row) = row else {
            return Ok(WorkspaceGuestCount::default());
        };
        Ok(WorkspaceGuestCount {
            collaborators: unsigned(&row, "guest_count")?,
            open_invites: unsigned(&row, "open_invite_count")?,
        })
    }

    pub async fn insert_workspace_invite(
        &self,
        invite: &WorkspaceInvite,
    ) -> Result<InviteInsertOutcome> {
        self.insert_workspace_invite_with_limit(invite, None).await
    }

    pub async fn insert_workspace_invite_within_cap(
        &self,
        invite: &WorkspaceInvite,
        max_guests: u32,
    ) -> Result<InviteInsertOutcome> {
        self.insert_workspace_invite_with_limit(invite, Some(max_guests))
            .await
    }

    async fn insert_workspace_invite_with_limit(
        &self,
        invite: &WorkspaceInvite,
        max_guests: Option<u32>,
    ) -> Result<InviteInsertOutcome> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if workspace_archived(&mut tx, tenant, &invite.workspace_id).await? == Some(true) {
            return Ok(InviteInsertOutcome::WorkspaceArchived);
        }
        let authorized: bool = sqlx::query_scalar(ISSUER_AUTHORIZED)
            .bind(tenant)
            .bind(&invite.created_by_principal_id.0)
            .bind(&invite.workspace_id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if !authorized {
            return Ok(InviteInsertOutcome::IssuerForbidden);
        }
        if let Some(max_guests) = max_guests {
            reconcile_invite_expiry(
                &mut tx,
                tenant,
                std::slice::from_ref(&invite.workspace_id.0),
                &now_iso(),
            )
            .await?;
            if committed_seats(&mut tx, tenant, &invite.workspace_id).await?
                >= u64::from(max_guests)
            {
                return Ok(InviteInsertOutcome::WorkspaceFull);
            }
        }
        let pin_identity = invite.pin_identity_key();
        let pin_github_user_id = invite.pin_github_user_id.or_else(|| {
            pin_identity
                .as_ref()
                .and_then(PrincipalIdentity::github_user_id)
        });
        let sql = format!(
            "INSERT INTO workspace_invite (tenant_id, {INVITE_COLUMNS}) VALUES ($1, $2, $3, $4, \
             $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(tenant)
            .bind(&invite.id)
            .bind(&invite.workspace_id.0)
            .bind(&invite.secret_hash)
            .bind(&invite.secret)
            .bind(&invite.created_by_principal_id.0)
            .bind(pin_github_user_id)
            .bind(&invite.pin_login)
            .bind(&invite.created_at)
            .bind(&invite.expires_at)
            .bind(&invite.redeemed_at)
            .bind(
                invite
                    .redeemed_by_principal_id
                    .as_ref()
                    .map(|p| p.0.as_str()),
            )
            .bind(&invite.revoked_at)
            .bind(to_i64(invite.redemption_count))
            .bind(pin_identity.as_ref().map(|i| i.provider.as_str()))
            .bind(pin_identity.as_ref().map(|i| i.host.as_str()))
            .bind(pin_identity.as_ref().map(|i| i.external_user_id.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("insert workspace invite failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(InviteInsertOutcome::Inserted)
    }

    pub async fn get_workspace_invite(&self, id: &str) -> Result<Option<WorkspaceInvite>> {
        let sql = format!(
            "SELECT {INVITE_COLUMNS} FROM workspace_invite WHERE tenant_id = $1 AND id = $2"
        );
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(workspace_invite).transpose()
    }

    pub async fn list_open_workspace_invites(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceInvite>> {
        let sql = format!(
            "SELECT {INVITE_COLUMNS} FROM workspace_invite i \
             WHERE i.tenant_id = $1 AND i.workspace_id = $2 AND {} \
             ORDER BY i.created_at COLLATE \"C\", i.id COLLATE \"C\"",
            invite_open(3)
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(now_iso())
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(workspace_invite).collect()
    }

    pub async fn revoke_workspace_invite(&self, id: &str) -> Result<bool> {
        let sql = format!(
            "UPDATE workspace_invite AS i SET revoked_at = $3 \
             WHERE i.tenant_id = $1 AND i.id = $2 AND {}",
            invite_open(3)
        );
        let mut tx = self.begin_write().await?;
        let revoked = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(id)
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(revoked > 0)
    }

    pub async fn archive_workspace_detaching_guests(
        &self,
        id: &WorkspaceId,
        archived_at: &str,
    ) -> Result<ArchivedGuestSweep> {
        self.archive_workspace_detaching_guests_inner(id, archived_at, false)
            .await?
            .ok_or_else(|| Error::Internal("manual archive refused".into()))
    }

    pub async fn archive_workspace_detaching_guests_unless_auto_archive_disabled(
        &self,
        id: &WorkspaceId,
        archived_at: &str,
    ) -> Result<Option<ArchivedGuestSweep>> {
        self.archive_workspace_detaching_guests_inner(id, archived_at, true)
            .await
    }

    async fn archive_workspace_detaching_guests_inner(
        &self,
        id: &WorkspaceId,
        archived_at: &str,
        respect_auto_archive_opt_out: bool,
    ) -> Result<Option<ArchivedGuestSweep>> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        // Read under the writer lock with the flip below, so a concurrent
        // `workspace.retire` cannot land in between: a retired row is never
        // flipped back to Archived.
        let state: Option<(bool, bool)> = sqlx::query_as(
            "SELECT status = $3, auto_archive_disabled <> 0 FROM workspace \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(&id.0)
        .bind(enum_to_db(&WorkspaceStatus::Retired)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("read archive status failed: {e}")))?;
        if state.is_some_and(|(retired, _)| retired) {
            return Err(Error::WorkspaceRetired {
                workspace_id: id.0.clone(),
            });
        }
        if respect_auto_archive_opt_out && state.is_some_and(|(_, opted_out)| opted_out) {
            return Ok(None);
        }
        let flipped = sqlx::query(
            "UPDATE workspace SET status = $3, archived = 1, archived_at = $4, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(&id.0)
        .bind(enum_to_db(&WorkspaceStatus::Archived)?)
        .bind(archived_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("archive workspace failed: {e}")))?;
        if flipped.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        // The workspace's home ends with the archive (node-link.md
        // "Home final stop"): its stop is recorded in this write.
        super::homes::retire_workspace_home(&mut tx, tenant, id, archived_at, false).await?;
        let removed_collaborators: Vec<PrincipalId> = sqlx::query_scalar::<_, String>(
            "SELECT principal_id FROM workspace_member m \
             WHERE m.tenant_id = $1 AND m.workspace_id = $2 AND m.role <> 'owner' \
             AND NOT EXISTS(SELECT 1 FROM host_member h \
                 WHERE h.tenant_id = $1 AND h.principal_id = m.principal_id) \
             ORDER BY m.added_at COLLATE \"C\", m.principal_id COLLATE \"C\"",
        )
        .bind(tenant)
        .bind(&id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("archive guest listing failed: {e}")))?
        .into_iter()
        .map(PrincipalId)
        .collect();
        let deleted = sqlx::query(
            "DELETE FROM workspace_member AS m \
             WHERE m.tenant_id = $1 AND m.workspace_id = $2 AND m.role <> 'owner' \
             AND NOT EXISTS(SELECT 1 FROM host_member h \
                 WHERE h.tenant_id = $1 AND h.principal_id = m.principal_id)",
        )
        .bind(tenant)
        .bind(&id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("archive guest detach failed: {e}")))?;
        if usize::try_from(deleted.rows_affected()).ok() != Some(removed_collaborators.len()) {
            return Err(Error::Internal(format!(
                "archive guest detach removed {} rows, expected {}",
                deleted.rows_affected(),
                removed_collaborators.len()
            )));
        }
        let revoke_sql = format!(
            "UPDATE workspace_invite AS i SET revoked_at = $3 \
             WHERE i.tenant_id = $1 AND i.workspace_id = $2 AND {}",
            invite_open(3)
        );
        let revoked = sqlx::query(sqlx::AssertSqlSafe(revoke_sql))
            .bind(tenant)
            .bind(&id.0)
            .bind(archived_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("archive invite revoke failed: {e}")))?;
        let remaining: i64 = sqlx::query_scalar(
            "SELECT CASE WHEN s.workspace_id = $3 THEN s.direct_count \
                 ELSE s.non_host_count + h.member_count END \
             FROM workspace_sharing_summary s CROSS JOIN host_membership_state h \
             WHERE s.tenant_id = $1 AND s.workspace_id = $2 AND h.tenant_id = $1 AND h.id = 1",
        )
        .bind(tenant)
        .bind(&id.0)
        .bind(CHIEF_WORKSPACE_ID)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("archive member count failed: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("archive workspace commit failed: {e}")))?;
        Ok(Some(ArchivedGuestSweep {
            removed_collaborators,
            revoked_invites: revoked.rows_affected(),
            member_count: u64::try_from(remaining).unwrap_or(0),
        }))
    }

    pub async fn redeem_workspace_invite(
        &self,
        id: &str,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let sql = format!(
            "UPDATE workspace_invite AS i SET redeemed_at = $3, redeemed_by_principal_id = $4, \
                 redemption_count = redemption_count + 1 \
             WHERE i.tenant_id = $1 AND i.id = $2 AND {}",
            invite_open(3)
        );
        let mut tx = self.begin_write().await?;
        let redeemed = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(id)
            .bind(now_iso())
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(redeemed > 0)
    }

    pub async fn join_workspace_by_invite(
        &self,
        invite_id: &str,
        workspace_id: &WorkspaceId,
        identity: &Principal,
        credential: HostJoinCredential<'_>,
        max_guests: u32,
    ) -> Result<InviteJoinOutcome> {
        let identity_key = identity
            .identity_key()
            .ok_or_else(|| Error::Internal("invite join requires an identity key".to_string()))?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let now = now_iso();
        if workspace_archived(&mut tx, tenant, workspace_id).await? == Some(true) {
            return Ok(InviteJoinOutcome::WorkspaceArchived);
        }
        let open_sql = format!(
            "SELECT {INVITE_COLUMNS} FROM workspace_invite i \
             WHERE i.tenant_id = $1 AND i.id = $2 AND i.workspace_id = $3 AND {}",
            invite_open(4)
        );
        let open = sqlx::query(sqlx::AssertSqlSafe(open_sql))
            .bind(tenant)
            .bind(invite_id)
            .bind(&workspace_id.0)
            .bind(&now)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(open) = open else {
            return Ok(InviteJoinOutcome::Closed);
        };
        let open = workspace_invite(&open)?;
        let authorized: bool = sqlx::query_scalar(ISSUER_AUTHORIZED)
            .bind(tenant)
            .bind(&open.created_by_principal_id.0)
            .bind(&workspace_id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if !authorized {
            return Ok(InviteJoinOutcome::Closed);
        }
        if open
            .pin_identity_key()
            .is_some_and(|pin| pin != identity_key)
        {
            return Ok(InviteJoinOutcome::PinMismatch);
        }
        let existing = principal_by_identity(&mut tx, tenant, &identity_key).await?;
        if identity.is_primary || existing.as_ref().is_some_and(|p| p.is_primary) {
            return Ok(InviteJoinOutcome::OwnerSelfJoin);
        }
        let effective_member = match &existing {
            Some(p) => is_host_member(&mut tx, tenant, &p.id).await?,
            None => false,
        };
        let already_member = effective_member
            || match &existing {
                Some(p) => member_role(&mut tx, tenant, workspace_id, &p.id)
                    .await?
                    .is_some(),
                None => false,
            };
        if !already_member {
            let collaborators: i64 = sqlx::query_scalar(
                "SELECT guest_count FROM workspace_sharing_summary \
                 WHERE tenant_id = $1 AND workspace_id = $2",
            )
            .bind(tenant)
            .bind(&workspace_id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            if u64::try_from(collaborators).unwrap_or(u64::MAX) >= u64::from(max_guests) {
                return Ok(InviteJoinOutcome::WorkspaceFull);
            }
        }
        let credential_hash = match credential {
            HostJoinCredential::Existing { token_hash } => {
                let Some(holder) = &existing else {
                    return Ok(InviteJoinOutcome::CredentialInvalid);
                };
                if !active_credential_of(&mut tx, tenant, token_hash, &holder.id).await? {
                    return Ok(InviteJoinOutcome::CredentialInvalid);
                }
                token_hash
            }
            HostJoinCredential::Proof {
                token_hash,
                authorization_generation,
            } => {
                let current = host_state(&mut tx, tenant).await?.authorization_generation;
                let revoked = match &existing {
                    Some(person) => revoked_generation(&mut tx, tenant, &person.id).await?,
                    None => None,
                };
                if authorization_generation > current
                    || revoked.is_some_and(|r| {
                        u64::try_from(r).unwrap_or(u64::MAX) > authorization_generation
                    })
                {
                    return Ok(InviteJoinOutcome::AccessRevoked);
                }
                token_hash
            }
        };
        if credential_hash.is_empty() {
            return Err(Error::InvalidInput("empty credential hash".into()));
        }
        let was_existing = existing.is_some();
        let mut principal = existing.unwrap_or_else(|| identity.clone());
        principal.github_user_id = identity_key.github_user_id();
        principal.identity = Some(identity_key.clone());
        principal.login.clone_from(&identity.login);
        principal.display_name.clone_from(&identity.display_name);
        principal.avatar_url.clone_from(&identity.avatar_url);
        principal.updated_at.clone_from(&now);
        write_principal(&mut tx, tenant, &principal, was_existing).await?;

        let redeem_sql = format!(
            "UPDATE workspace_invite AS i SET redeemed_at = $4, redeemed_by_principal_id = $5, \
                 redemption_count = redemption_count + $6 \
             WHERE i.tenant_id = $1 AND i.id = $2 AND i.workspace_id = $3 AND {}",
            invite_open(4)
        );
        let redeemed = sqlx::query(sqlx::AssertSqlSafe(redeem_sql))
            .bind(tenant)
            .bind(invite_id)
            .bind(&workspace_id.0)
            .bind(&now)
            .bind(&principal.id.0)
            .bind(i64::from(!already_member))
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        if redeemed == 0 {
            return Err(Error::Internal(format!(
                "invite {invite_id} closed inside its own join transaction"
            )));
        }
        if !effective_member {
            let sql = format!(
                "INSERT INTO workspace_member (tenant_id, {MEMBER_COLUMNS}) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (tenant_id, workspace_id, principal_id) DO NOTHING"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(tenant)
                .bind(&workspace_id.0)
                .bind(&principal.id.0)
                .bind(WorkspaceRole::Collaborator.as_str())
                .bind(&now)
                .execute(&mut *tx)
                .await
                .map_err(|e| map_owner_violation(&e, workspace_id, "invite join add member"))?;
        }
        if matches!(credential, HostJoinCredential::Proof { .. }) {
            insert_credential(&mut tx, tenant, credential_hash, &principal.id, &now).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(if already_member {
            InviteJoinOutcome::Rejoined(principal)
        } else {
            InviteJoinOutcome::Joined(principal)
        })
    }

    // Host membership (`host_membership_repo.rs`).

    pub async fn principal_instructions_revoked(&self, principal: &PrincipalId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let revoked = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM principal_revocation WHERE tenant_id = $1 \
                 AND principal_id = $2) \
             AND NOT EXISTS(SELECT 1 FROM principal_credential WHERE tenant_id = $1 \
                 AND principal_id = $2 AND revoked_at IS NULL)",
        )
        .bind(self.tenant())
        .bind(&principal.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(revoked)
    }

    pub async fn host_membership_state(&self) -> Result<HostMembershipState> {
        let mut tx = self.begin().await?;
        let state = host_state(&mut tx, self.tenant()).await?;
        tx.commit().await.map_err(db)?;
        Ok(state)
    }

    pub async fn get_host_role(&self, principal_id: &PrincipalId) -> Result<HostRole> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT p.is_primary, h.principal_id IS NOT NULL AS is_member \
             FROM principal p LEFT JOIN host_member h ON h.tenant_id = $1 AND h.principal_id = p.id \
             WHERE p.tenant_id = $1 AND p.id = $2",
        )
        .bind(self.tenant())
        .bind(&principal_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound(format!("principal {principal_id}")))?;
        tx.commit().await.map_err(db)?;
        Ok(host_role(
            col::<i64>(&row, "is_primary")? != 0,
            col(&row, "is_member")?,
        ))
    }

    pub async fn list_host_members(&self) -> Result<HostMembersSnapshot> {
        let added = "CASE WHEN p.is_primary = 1 THEN p.created_at ELSE h.added_at END";
        let sql = format!(
            "SELECT p.id, p.is_primary, p.login, p.display_name, p.avatar_url, \
                    p.identity_provider, p.instance_host, p.external_user_id, \
                    {added} AS added_at, s.revision \
             FROM principal p LEFT JOIN host_member h ON h.tenant_id = $1 AND h.principal_id = p.id \
             CROSS JOIN host_membership_state s \
             WHERE p.tenant_id = $1 AND s.tenant_id = $1 AND s.id = 1 \
                 AND (p.is_primary = 1 OR h.principal_id IS NOT NULL) \
             ORDER BY p.is_primary DESC, ({added}) COLLATE \"C\", p.id COLLATE \"C\""
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let revision = rows
            .first()
            .map(|row| unsigned(row, "revision"))
            .transpose()?
            .unwrap_or(0);
        let members = rows
            .iter()
            .map(|row| {
                Ok(HostMember {
                    principal_id: PrincipalId(col(row, "id")?),
                    host_role: if col::<i64>(row, "is_primary")? != 0 {
                        HostRole::Owner
                    } else {
                        HostRole::Member
                    },
                    login: col(row, "login")?,
                    display_name: col(row, "display_name")?,
                    avatar_url: col(row, "avatar_url")?,
                    identity: col::<Option<String>>(row, "identity_provider")?
                        .map(|provider| {
                            Ok::<_, Error>(PrincipalIdentity {
                                provider,
                                host: col(row, "instance_host")?,
                                external_user_id: col(row, "external_user_id")?,
                            })
                        })
                        .transpose()?,
                    added_at: col(row, "added_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(HostMembersSnapshot { members, revision })
    }

    pub async fn insert_host_invite(&self, invite: &HostInvite) -> Result<()> {
        invite.validate_new()?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if !is_primary(&mut tx, tenant, &invite.created_by_principal_id).await? {
            return Err(Error::InvalidInput(
                "only the host owner may issue host invitations".into(),
            ));
        }
        let sql = format!(
            "INSERT INTO host_invite (tenant_id, {HOST_INVITE_COLUMNS}) VALUES ($1, $2, $3, $4, \
             $5, $6, $7, $8, $9, $10, $11, NULL, NULL, NULL, 0)"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(tenant)
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
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    async fn host_invite_in(
        conn: &mut PgConnection,
        tenant: &str,
        id: &str,
    ) -> Result<Option<HostInvite>> {
        let sql = format!(
            "SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE tenant_id = $1 AND id = $2"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(tenant)
            .bind(id)
            .fetch_optional(conn)
            .await
            .map_err(db)?
            .as_ref()
            .map(host_invite)
            .transpose()
    }

    pub async fn get_host_invite(&self, id: &str) -> Result<Option<HostInvite>> {
        let mut tx = self.begin().await?;
        let invite = Self::host_invite_in(&mut tx, self.tenant(), id).await?;
        tx.commit().await.map_err(db)?;
        Ok(invite)
    }

    pub async fn list_open_host_invites(&self) -> Result<Vec<HostInvite>> {
        let now = now_iso();
        let sql = format!(
            "SELECT {HOST_INVITE_COLUMNS} FROM host_invite WHERE tenant_id = $1 \
             AND revoked_at IS NULL AND redeemed_at IS NULL AND redemption_count = 0 \
             AND sqlite_julianday(expires_at) >= sqlite_julianday($2) \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&now)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let invites = rows.iter().map(host_invite).collect::<Result<Vec<_>>>()?;
        Ok(invites
            .into_iter()
            .filter(|invite| invite.is_open_at(&now))
            .collect())
    }

    pub async fn revoke_host_invite(&self, id: &str) -> Result<bool> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let Some(invite) = Self::host_invite_in(&mut tx, tenant, id).await? else {
            return Err(Error::NotFound(format!("host invite {id}")));
        };
        let now = now_iso();
        if !invite.is_open_at(&now) {
            return Ok(false);
        }
        sqlx::query("UPDATE host_invite SET revoked_at = $3 WHERE tenant_id = $1 AND id = $2")
            .bind(tenant)
            .bind(id)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn join_host_by_invite(
        &self,
        invite_id: &str,
        identity: &Principal,
        credential: HostJoinCredential<'_>,
    ) -> Result<HostInviteJoinOutcome> {
        let key = identity
            .identity_key()
            .ok_or_else(|| Error::InvalidInput("host join requires a verified identity".into()))?;
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        let Some(invite) = Self::host_invite_in(&mut tx, tenant, invite_id).await? else {
            return Ok(HostInviteJoinOutcome::NotFound);
        };
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
        let existing = principal_by_identity(&mut tx, tenant, &key).await?;
        if existing.as_ref().is_some_and(|p| p.is_primary) || identity.is_primary {
            return Ok(HostInviteJoinOutcome::OwnerSelfJoin);
        }
        let state = host_state(&mut tx, tenant).await?;
        let token_hash = match credential {
            HostJoinCredential::Proof {
                token_hash,
                authorization_generation,
            } => {
                let revoked = match &existing {
                    Some(p) => revoked_generation(&mut tx, tenant, &p.id).await?,
                    None => None,
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
                let Some(p) = &existing else {
                    return Ok(HostInviteJoinOutcome::CredentialInvalid);
                };
                if !active_credential_of(&mut tx, tenant, token_hash, &p.id).await? {
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
        write_principal(&mut tx, tenant, &principal, was_existing).await?;
        let membership_added = sqlx::query(
            "INSERT INTO host_member (tenant_id, principal_id, added_at) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, principal_id) DO NOTHING",
        )
        .bind(tenant)
        .bind(&principal.id.0)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            != 0;
        sqlx::query(
            "UPDATE host_invite SET redeemed_at = $3, redeemed_by_principal_id = $4, \
             redemption_count = 1 WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(invite_id)
        .bind(&now)
        .bind(&principal.id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if matches!(credential, HostJoinCredential::Proof { .. }) {
            insert_credential(&mut tx, tenant, token_hash, &principal.id, &now).await?;
        }
        let revision = host_state(&mut tx, tenant).await?.revision;
        tx.commit().await.map_err(db)?;
        Ok(HostInviteJoinOutcome::Joined {
            principal: Box::new(principal),
            membership_added,
            revision,
        })
    }

    pub async fn remove_host_member(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<HostMemberRemoval> {
        self.remove_host_member_inner(principal_id, None, false)
            .await
    }

    pub async fn remove_host_member_by_owner(
        &self,
        actor: &PrincipalId,
        principal_id: &PrincipalId,
    ) -> Result<HostMemberRemoval> {
        self.remove_host_member_inner(principal_id, Some(actor), false)
            .await
    }

    pub async fn revoke_principal_access(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<HostMemberRemoval> {
        self.remove_host_member_inner(principal_id, None, true)
            .await
    }

    async fn remove_host_member_inner(
        &self,
        principal_id: &PrincipalId,
        owner: Option<&PrincipalId>,
        revoke_guest: bool,
    ) -> Result<HostMemberRemoval> {
        let tenant = self.tenant();
        let mut tx = self.begin_write().await?;
        if let Some(owner) = owner {
            if !is_primary(&mut tx, tenant, owner).await? {
                return Err(Error::Forbidden(
                    "host.members.remove requires the host owner".into(),
                ));
            }
        }
        if is_primary(&mut tx, tenant, principal_id).await? {
            return Err(Error::InvalidInput(
                "the host owner cannot be removed".into(),
            ));
        }
        let removed =
            sqlx::query("DELETE FROM host_member WHERE tenant_id = $1 AND principal_id = $2")
                .bind(tenant)
                .bind(&principal_id.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected()
                != 0;
        let mut result = HostMemberRemoval {
            removed,
            revision: host_state(&mut tx, tenant).await?.revision,
            credentials: 0,
            workspaces: Vec::new(),
            revoked_invites: Vec::new(),
            affected_workspaces: Vec::new(),
        };
        if removed || revoke_guest {
            if removed {
                let ids: Vec<String> = sqlx::query_scalar(
                    "SELECT id FROM workspace WHERE tenant_id = $1 AND id <> $2 \
                     AND substr(id, 1, 9) <> '__home__:' ORDER BY id COLLATE \"C\"",
                )
                .bind(tenant)
                .bind(CHIEF_WORKSPACE_ID)
                .fetch_all(&mut *tx)
                .await
                .map_err(db)?;
                result.affected_workspaces = ids.into_iter().map(WorkspaceId).collect();
            }
            let now = now_iso();
            sqlx::query(
                "UPDATE host_membership_state SET authorization_generation = \
                 authorization_generation + 1 WHERE tenant_id = $1 AND id = 1",
            )
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            sqlx::query(
                "INSERT INTO principal_revocation (tenant_id, principal_id, generation) \
                 SELECT $1, $2, authorization_generation FROM host_membership_state \
                 WHERE tenant_id = $1 AND id = 1 \
                 ON CONFLICT (tenant_id, principal_id) DO UPDATE SET generation = excluded.generation",
            )
            .bind(tenant)
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            result.credentials = revoke_credentials_of(&mut tx, tenant, principal_id, &now).await?;
            let mut workspaces: Vec<String> = sqlx::query_scalar(
                "DELETE FROM workspace_member WHERE tenant_id = $1 AND principal_id = $2 \
                 AND (role IN ('collaborator', 'manager') OR $3) RETURNING workspace_id",
            )
            .bind(tenant)
            .bind(&principal_id.0)
            .bind(removed)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            workspaces.sort();
            result.workspaces = workspaces.into_iter().map(WorkspaceId).collect();
            if removed {
                let revoke = format!(
                    "UPDATE workspace_invite AS i SET revoked_at = $3 \
                     WHERE i.tenant_id = $1 AND i.created_by_principal_id = $2 AND {} \
                     RETURNING id",
                    invite_open(3)
                );
                let mut revoked: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(revoke))
                    .bind(tenant)
                    .bind(&principal_id.0)
                    .bind(&now)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(db)?;
                revoked.sort();
                result.revoked_invites = revoked;
            }
            sqlx::query(
                "DELETE FROM agent_queue WHERE tenant_id = $1 AND CASE WHEN \
                 sqlite_json_valid(payload) THEN sqlite_json_extract(payload, \
                 '$.messageMetadata.fromPrincipalId') END = $2",
            )
            .bind(tenant)
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
}

async fn revoke_credentials_of(
    tx: &mut WriteTx,
    tenant: &str,
    principal_id: &PrincipalId,
    now: &str,
) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE principal_credential SET revoked_at = $3 \
         WHERE tenant_id = $1 AND principal_id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant)
    .bind(&principal_id.0)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(db)?
    .rows_affected())
}
