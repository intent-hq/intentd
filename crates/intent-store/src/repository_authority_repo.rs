//! Durable continuity evidence for repository admission, not a permission policy.
//!
//! Every read is one `SQLite` snapshot, released before returning. The caller must
//! still apply the existing service gates and independently validate the original
//! wire/legacy credential, root, operation and forge connection lifetimes.

use intent_core::{PrincipalId, WorkspaceId, WorkspaceRole};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

use crate::{enum_from_db, Error, Result, Store};

/// Positive, checked `SQLite` counter. Compare only for the same database and key.
/// This private-service evidence has no wire representation or timestamp fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityRevision(u64);

impl AuthorityRevision {
    /// The exact durable counter; never combine independent revisions into a hash.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Current row plus continuity. A deleted row retains its revision; a key that
/// never existed has neither. A present row without valid provenance is an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedAuthority<T> {
    /// Durable revision of this exact key, including deletions.
    pub revision: Option<AuthorityRevision>,
    /// Current facts; absence never confers authority.
    pub value: Option<T>,
}

/// Workspace facts needed alongside its incarnation/ownership revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryWorkspaceAuthority {
    /// Actual owner column, without inferring a replacement owner.
    pub owner_principal_id: Option<PrincipalId>,
}

/// Principal authority fields stored verbatim, without profile metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryPrincipalAuthority {
    /// Durable principal ID.
    pub id: PrincipalId,
    /// Actual primary flag; the existing service gate decides its meaning.
    pub is_primary: bool,
    /// Legacy GitHub identity, retained independently of the neutral triple.
    pub github_user_id: Option<i64>,
    /// Actual provider column.
    pub identity_provider: Option<String>,
    /// Actual logical instance column.
    pub instance_host: Option<String>,
    /// Actual account column.
    pub external_user_id: Option<String>,
}

/// Facts about the exact original credential. Contains no bearer or token hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryCredentialAuthority {
    /// Actual credential owner, which must match the original caller.
    pub principal_id: PrincipalId,
    /// Whether this credential has been revoked.
    pub revoked: bool,
}

/// One consistent snapshot of durable authority inputs. No lease escapes this
/// read, and this is not a grant, even when all the rows exist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryAuthoritySnapshot {
    /// Originally requested workspace, never a current UI/default workspace.
    pub workspace_id: WorkspaceId,
    /// Originally admitted principal, never a substitute primary.
    pub principal_id: PrincipalId,
    /// Workspace incarnation and owner continuity.
    pub workspace: VersionedAuthority<RepositoryWorkspaceAuthority>,
    /// Original principal incarnation and identity/primary continuity.
    pub principal: VersionedAuthority<RepositoryPrincipalAuthority>,
    /// Current primary and its own continuity, from this same transaction.
    pub primary_principal: VersionedAuthority<RepositoryPrincipalAuthority>,
    /// Explicit host grant. Owner/member/guest interpretation stays in services.
    pub host_member: VersionedAuthority<()>,
    /// Explicit workspace grant; inherited permissions stay in services.
    pub workspace_grant: VersionedAuthority<WorkspaceRole>,
    /// `None` means no original personal hash was supplied, not authorization.
    pub credential: Option<VersionedAuthority<RepositoryCredentialAuthority>>,
    /// Existing host revocation generation, kept distinct from all row revisions.
    pub host_authorization_generation: u64,
    /// Existing revocation evidence for the original principal, if any.
    pub principal_revocation_generation: Option<u64>,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Result::map_err passes its owned database error"
)]
fn db_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("repository authority snapshot failed: {error}"))
}

fn counter(value: i64, positive: bool) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|v| !positive || *v > 0)
        .ok_or_else(|| Error::Internal("invalid repository authority provenance".into()))
}

async fn versioned<T>(
    conn: &mut SqliteConnection,
    kind: &str,
    key: &str,
    member: &str,
    value: Option<T>,
) -> Result<VersionedAuthority<T>> {
    let raw: Option<i64> = sqlx::query_scalar(
        "SELECT revision FROM repository_authority_revision \
         WHERE kind = ? AND subject_id = ? AND member_id = ?",
    )
    .bind(kind)
    .bind(key)
    .bind(member)
    .fetch_optional(conn)
    .await
    .map_err(db_error)?;
    let revision = raw
        .map(|n| counter(n, true).map(AuthorityRevision))
        .transpose()?;
    if value.is_some() && revision.is_none() {
        return Err(Error::Internal(
            "missing repository authority provenance".into(),
        ));
    }
    Ok(VersionedAuthority { revision, value })
}

fn principal_row(row: &SqliteRow) -> Result<RepositoryPrincipalAuthority> {
    Ok(RepositoryPrincipalAuthority {
        id: PrincipalId(row.try_get("id").map_err(db_error)?),
        is_primary: row.try_get("is_primary").map_err(db_error)?,
        github_user_id: row.try_get("github_user_id").map_err(db_error)?,
        identity_provider: row.try_get("identity_provider").map_err(db_error)?,
        instance_host: row.try_get("instance_host").map_err(db_error)?,
        external_user_id: row.try_get("external_user_id").map_err(db_error)?,
    })
}

async fn principal(
    conn: &mut SqliteConnection,
    id: &PrincipalId,
) -> Result<VersionedAuthority<RepositoryPrincipalAuthority>> {
    let row = sqlx::query(
        "SELECT id, is_primary, github_user_id, identity_provider, instance_host, external_user_id \
         FROM principal WHERE id = ?",
    )
    .bind(id.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(db_error)?;
    let value = row.as_ref().map(principal_row).transpose()?;
    versioned(conn, "principal", id.as_str(), "", value).await
}

impl Store {
    /// Read exact original workspace/principal/optional personal credential facts
    /// and independent ABA-resistant revisions in one read transaction, closed
    /// before returning. No tokens/hashes are copied into the result. `None` for
    /// the hash does not authorize a local/legacy caller. Root, wire credential,
    /// process, settings and forge-directory facts are deliberately outside this
    /// snapshot and require their own revalidation by the operation owner.
    ///
    /// # Errors
    /// Returns `Internal` on storage failure or missing/invalid provenance.
    pub async fn repository_authority_snapshot(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        original_token_hash: Option<&str>,
    ) -> Result<RepositoryAuthoritySnapshot> {
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let snapshot =
            read_snapshot(&mut tx, workspace_id, principal_id, original_token_hash).await?;
        tx.commit().await.map_err(db_error)?;
        Ok(snapshot)
    }
}

async fn read_snapshot(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    principal_id: &PrincipalId,
    original_token_hash: Option<&str>,
) -> Result<RepositoryAuthoritySnapshot> {
    let row = sqlx::query("SELECT owner_principal_id FROM workspace WHERE id = ?")
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_error)?;
    let workspace = versioned(
        conn,
        "workspace",
        workspace_id.as_str(),
        "",
        row.map(|r| {
            r.try_get::<Option<String>, _>("owner_principal_id")
                .map(|owner| RepositoryWorkspaceAuthority {
                    owner_principal_id: owner.map(PrincipalId),
                })
                .map_err(db_error)
        })
        .transpose()?,
    )
    .await?;
    let person = principal(conn, principal_id).await?;
    let primary_id: Option<String> =
        sqlx::query_scalar("SELECT id FROM principal WHERE is_primary = 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_error)?;
    let primary_principal = if let Some(id) = primary_id {
        principal(conn, &PrincipalId(id)).await?
    } else {
        VersionedAuthority {
            revision: None,
            value: None,
        }
    };
    let host: Option<i64> = sqlx::query_scalar("SELECT 1 FROM host_member WHERE principal_id = ?")
        .bind(principal_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_error)?;
    let host_member = versioned(
        conn,
        "host_member",
        principal_id.as_str(),
        "",
        host.map(|_| ()),
    )
    .await?;
    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM workspace_member WHERE workspace_id = ? AND principal_id = ?",
    )
    .bind(workspace_id.as_str())
    .bind(principal_id.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(db_error)?;
    let workspace_grant = versioned(
        conn,
        "workspace_member",
        workspace_id.as_str(),
        principal_id.as_str(),
        role.as_deref().map(enum_from_db).transpose()?,
    )
    .await?;
    let credential = if let Some(hash) = original_token_hash {
        let row = sqlx::query("SELECT principal_id, revoked_at IS NOT NULL AS revoked FROM principal_credential WHERE token_hash = ?")
            .bind(hash).fetch_optional(&mut *conn).await.map_err(db_error)?;
        let value = row
            .map(|r| {
                Ok(RepositoryCredentialAuthority {
                    principal_id: PrincipalId(r.try_get("principal_id").map_err(db_error)?),
                    revoked: r.try_get("revoked").map_err(db_error)?,
                })
            })
            .transpose()?;
        Some(versioned(conn, "credential", hash, "", value).await?)
    } else {
        None
    };
    let host_generation: i64 = sqlx::query_scalar(
        "SELECT authorization_generation FROM host_membership_state WHERE id = 1",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(db_error)?;
    let revocation: Option<i64> =
        sqlx::query_scalar("SELECT generation FROM principal_revocation WHERE principal_id = ?")
            .bind(principal_id.as_str())
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_error)?;
    Ok(RepositoryAuthoritySnapshot {
        workspace_id: workspace_id.clone(),
        principal_id: principal_id.clone(),
        workspace,
        principal: person,
        primary_principal,
        host_member,
        workspace_grant,
        credential,
        host_authorization_generation: counter(host_generation, false)?,
        principal_revocation_generation: revocation.map(|n| counter(n, true)).transpose()?,
    })
}

#[cfg(test)]
mod tests;
