//! Principal, workspace membership and bearer-credential repository
//! (multiplayer w1, migration `0125_principals`). Principals are people
//! (GitHub identities); the primary principal is minted by the migration and
//! owns every pre-existing workspace. Credentials are keyed by the hex
//! SHA-256 of the presented token — the service layer hashes, this module
//! never sees plaintext.

use std::collections::HashMap;

use intent_core::{
    now_iso, Error, Principal, PrincipalCredential, PrincipalId, Result, WorkspaceId,
    WorkspaceMember, WorkspaceMembership, WorkspaceRole,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, Store};

const PRINCIPAL_COLUMNS: &str =
    "id, github_user_id, login, display_name, avatar_url, is_primary, created_at, updated_at";

const MEMBER_COLUMNS: &str = "workspace_id, principal_id, role, added_at";

const CREDENTIAL_COLUMNS: &str = "token_hash, principal_id, created_at, last_used_at, revoked_at";

impl Store {
    /// Fetch a principal by id.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when no such principal exists and
    /// `Error::Internal` if the database operation fails.
    pub async fn get_principal(&self, id: &PrincipalId) -> Result<Principal> {
        let sql = format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get principal failed: {e}")))?;
        row.as_ref()
            .map(map_principal_row)
            .ok_or_else(|| Error::NotFound(format!("principal {id}")))
    }

    /// The daemon's primary principal (the original single user, minted by
    /// migration `0125_principals`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the row is missing (a broken database —
    /// the migration guarantees exactly one) or the database operation fails.
    pub async fn get_primary_principal(&self) -> Result<Principal> {
        let sql = format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE is_primary = 1");
        let row = sqlx::query(&sql)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get primary principal failed: {e}")))?;
        row.as_ref()
            .map(map_principal_row)
            .ok_or_else(|| Error::Internal("primary principal missing".to_string()))
    }

    /// Look up a principal by linked GitHub account id; `None` when no
    /// principal has linked that account.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn find_principal_by_github_user_id(
        &self,
        github_user_id: i64,
    ) -> Result<Option<Principal>> {
        let sql = format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE github_user_id = ?");
        let row = sqlx::query(&sql)
            .bind(github_user_id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("find principal by github id failed: {e}")))?;
        Ok(row.as_ref().map(map_principal_row))
    }

    /// List every principal, primary first then by `created_at`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_principals(&self) -> Result<Vec<Principal>> {
        let sql = format!(
            "SELECT {PRINCIPAL_COLUMNS} FROM principal ORDER BY is_primary DESC, created_at, id"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list principals failed: {e}")))?;
        Ok(rows.iter().map(map_principal_row).collect())
    }

    /// Insert or update a principal by id. On conflict the GitHub identity
    /// and cached profile fields are overwritten and `updated_at` bumped;
    /// `is_primary` and `created_at` are never changed by an upsert (the
    /// primary flag is owned by the migration).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails (including
    /// a `github_user_id` already linked to another principal).
    pub async fn upsert_principal(&self, p: &Principal) -> Result<()> {
        let sql = format!(
            "INSERT INTO principal ({PRINCIPAL_COLUMNS}) VALUES (?,?,?,?,?,?,?,?) \
             ON CONFLICT(id) DO UPDATE SET \
                 github_user_id = excluded.github_user_id, \
                 login = excluded.login, \
                 display_name = excluded.display_name, \
                 avatar_url = excluded.avatar_url, \
                 updated_at = excluded.updated_at"
        );
        sqlx::query(&sql)
            .bind(&p.id.0)
            .bind(p.github_user_id)
            .bind(&p.login)
            .bind(&p.display_name)
            .bind(&p.avatar_url)
            .bind(i64::from(p.is_primary))
            .bind(&p.created_at)
            .bind(&p.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("upsert principal failed: {e}")))?;
        Ok(())
    }

    /// The workspace's `owner_principal_id` column; `None` only for a
    /// workspace that does not exist (the insert trigger always fills it).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_owner_principal_id(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<PrincipalId>> {
        let row = sqlx::query("SELECT owner_principal_id FROM workspace WHERE id = ?")
            .bind(&workspace_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace owner failed: {e}")))?;
        Ok(row
            .and_then(|r| r.get::<Option<String>, _>("owner_principal_id"))
            .map(PrincipalId))
    }

    /// Membership summaries for `workspace.get` / `workspace.list`
    /// (multiplayer w1): owner, member count and `viewer`'s role, computed
    /// in SQL in ONE query for every workspace (or just `workspace_id`),
    /// keyed by workspace id. `viewer = None` yields no `my_role`.
    /// `open_invite_count` is `0` until invitations exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn workspace_membership_summaries(
        &self,
        viewer: Option<&PrincipalId>,
        workspace_id: Option<&WorkspaceId>,
    ) -> Result<HashMap<WorkspaceId, WorkspaceMembership>> {
        let mut sql = String::from(
            "SELECT w.id AS workspace_id, w.owner_principal_id, \
                (SELECT COUNT(*) FROM workspace_member m WHERE m.workspace_id = w.id) AS member_count, \
                (SELECT m.role FROM workspace_member m \
                    WHERE m.workspace_id = w.id AND m.principal_id = ?) AS my_role \
             FROM workspace w",
        );
        if workspace_id.is_some() {
            sql.push_str(" WHERE w.id = ?");
        }
        let mut query = sqlx::query(&sql).bind(viewer.map(|p| p.0.as_str()));
        if let Some(id) = workspace_id {
            query = query.bind(&id.0);
        }
        let rows = query
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("workspace membership summaries failed: {e}")))?;
        rows.iter()
            .map(|r| {
                let my_role = r
                    .get::<Option<String>, _>("my_role")
                    .map(|role| enum_from_db::<WorkspaceRole>(&role))
                    .transpose()?;
                Ok((
                    WorkspaceId(r.get("workspace_id")),
                    WorkspaceMembership {
                        owner_principal_id: r
                            .get::<Option<String>, _>("owner_principal_id")
                            .map(PrincipalId),
                        my_role,
                        member_count: u64::try_from(r.get::<i64, _>("member_count")).unwrap_or(0),
                        open_invite_count: 0,
                    },
                ))
            })
            .collect()
    }

    /// List a workspace's members, owners first then by `added_at`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspace_members(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceMember>> {
        let sql = format!(
            "SELECT {MEMBER_COLUMNS} FROM workspace_member WHERE workspace_id = ? \
             ORDER BY CASE role WHEN 'owner' THEN 0 ELSE 1 END, added_at, principal_id"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspace members failed: {e}")))?;
        rows.iter().map(map_member_row).collect()
    }

    /// List every workspace id a principal is a member of, with the role.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_principal_memberships(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<Vec<WorkspaceMember>> {
        let sql = format!(
            "SELECT {MEMBER_COLUMNS} FROM workspace_member WHERE principal_id = ? \
             ORDER BY added_at, workspace_id"
        );
        let rows = sqlx::query(&sql)
            .bind(&principal_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list principal memberships failed: {e}")))?;
        rows.iter().map(map_member_row).collect()
    }

    /// Add a principal to a workspace with `role`. Idempotent: an existing
    /// membership is left untouched (use
    /// [`Store::set_workspace_member_role`] to change its role). Returns
    /// whether a row was inserted.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails (including
    /// an unknown workspace or principal, rejected by the FKs).
    pub async fn add_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<bool> {
        let sql = format!(
            "INSERT INTO workspace_member ({MEMBER_COLUMNS}) VALUES (?,?,?,?) \
             ON CONFLICT(workspace_id, principal_id) DO NOTHING"
        );
        let res = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .bind(role.as_str())
            .bind(now_iso())
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("add workspace member failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Change an existing member's role.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when the principal is not a member of the
    /// workspace and `Error::Internal` if the database operation fails.
    pub async fn set_workspace_member_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<()> {
        let res = sqlx::query(
            "UPDATE workspace_member SET role = ? WHERE workspace_id = ? AND principal_id = ?",
        )
        .bind(role.as_str())
        .bind(&workspace_id.0)
        .bind(&principal_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set workspace member role failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!(
                "principal {principal_id} is not a member of workspace {workspace_id}"
            )));
        }
        Ok(())
    }

    /// Remove a principal from a workspace. Returns whether a row was
    /// removed; removing a non-member is not an error.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn remove_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let res =
            sqlx::query("DELETE FROM workspace_member WHERE workspace_id = ? AND principal_id = ?")
                .bind(&workspace_id.0)
                .bind(&principal_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("remove workspace member failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Record a bearer credential for a principal, keyed by `token_hash`
    /// (hex SHA-256 of the token — never the token itself).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails (including
    /// a duplicate hash).
    pub async fn insert_principal_credential(
        &self,
        principal_id: &PrincipalId,
        token_hash: &str,
    ) -> Result<PrincipalCredential> {
        let now = now_iso();
        sqlx::query(
            "INSERT INTO principal_credential (token_hash, principal_id, created_at) \
             VALUES (?,?,?)",
        )
        .bind(token_hash)
        .bind(&principal_id.0)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("insert principal credential failed: {e}")))?;
        Ok(PrincipalCredential {
            token_hash: token_hash.to_string(),
            principal_id: principal_id.clone(),
            created_at: now,
            last_used_at: None,
            revoked_at: None,
        })
    }

    /// Look up a credential by token hash, revoked or not (`None` when the
    /// hash is unknown). Callers gate on
    /// [`PrincipalCredential::is_active`].
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn lookup_principal_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<PrincipalCredential>> {
        let sql =
            format!("SELECT {CREDENTIAL_COLUMNS} FROM principal_credential WHERE token_hash = ?");
        let row = sqlx::query(&sql)
            .bind(token_hash)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("lookup principal credential failed: {e}")))?;
        Ok(row.as_ref().map(map_credential_row))
    }

    /// List a principal's credentials, newest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_principal_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<Vec<PrincipalCredential>> {
        let sql = format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM principal_credential WHERE principal_id = ? \
             ORDER BY created_at DESC, token_hash"
        );
        let rows = sqlx::query(&sql)
            .bind(&principal_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list principal credentials failed: {e}")))?;
        Ok(rows.iter().map(map_credential_row).collect())
    }

    /// Bump `last_used_at` on an active credential. Returns whether a row was
    /// touched (`false` for an unknown or revoked hash).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn touch_principal_credential(&self, token_hash: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE principal_credential SET last_used_at = ? \
             WHERE token_hash = ? AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(token_hash)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("touch principal credential failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Revoke a credential by token hash. Idempotent: returns whether the
    /// row flipped from active to revoked (`false` for an unknown or
    /// already-revoked hash).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn revoke_principal_credential(&self, token_hash: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE principal_credential SET revoked_at = ? \
             WHERE token_hash = ? AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(token_hash)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("revoke principal credential failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

fn map_principal_row(r: &SqliteRow) -> Principal {
    Principal {
        id: PrincipalId(r.get("id")),
        github_user_id: r.get("github_user_id"),
        login: r.get("login"),
        display_name: r.get("display_name"),
        avatar_url: r.get("avatar_url"),
        is_primary: r.get::<i64, _>("is_primary") != 0,
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

fn map_member_row(r: &SqliteRow) -> Result<WorkspaceMember> {
    Ok(WorkspaceMember {
        workspace_id: WorkspaceId(r.get("workspace_id")),
        principal_id: PrincipalId(r.get("principal_id")),
        role: enum_from_db::<WorkspaceRole>(r.get::<String, _>("role").as_str())?,
        added_at: r.get("added_at"),
    })
}

fn map_credential_row(r: &SqliteRow) -> PrincipalCredential {
    PrincipalCredential {
        token_hash: r.get("token_hash"),
        principal_id: PrincipalId(r.get("principal_id")),
        created_at: r.get("created_at"),
        last_used_at: r.get("last_used_at"),
        revoked_at: r.get("revoked_at"),
    }
}
