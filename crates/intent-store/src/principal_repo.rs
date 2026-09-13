//! Principal, workspace membership and bearer-credential repository
//! (multiplayer w1, migration `0125_principals`). Principals are people
//! (GitHub identities); the primary principal is minted by the migration and
//! owns every pre-existing workspace. Credentials are keyed by the hex
//! SHA-256 of the presented token — the service layer hashes, this module
//! never sees plaintext.

use std::collections::HashMap;

use intent_core::{
    now_iso, Error, Principal, PrincipalCredential, PrincipalId, Result, WorkspaceId,
    WorkspaceInvite, WorkspaceMember, WorkspaceMembership, WorkspaceRole,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, Store};

const PRINCIPAL_COLUMNS: &str =
    "id, github_user_id, login, display_name, avatar_url, is_primary, created_at, updated_at";

const MEMBER_COLUMNS: &str = "workspace_id, principal_id, role, added_at";

const CREDENTIAL_COLUMNS: &str = "token_hash, principal_id, created_at, last_used_at, revoked_at";

const INVITE_COLUMNS: &str = "id, workspace_id, secret_hash, created_by_principal_id, \
     pin_github_user_id, pin_login, created_at, expires_at, redeemed_at, \
     redeemed_by_principal_id, revoked_at";

/// The workspace columns an unstamped user message's author is resolved
/// from (see [`Store::get_workspace_author_fallback`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceAuthorFallback {
    /// The principal pre-multiplayer content is credited to; `None` for a
    /// workspace created after migration `0125`.
    pub legacy_author_principal_id: Option<PrincipalId>,
    /// The current owner; `None` only for a transfer-imported row whose
    /// principal columns were not yet re-derived.
    pub owner_principal_id: Option<PrincipalId>,
}

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

    /// Fetch the principals in `ids` that exist, in one `IN (...)` statement
    /// per chunk of `IDS_PER_STATEMENT` (below the `SQLite` bound-variable
    /// limit). Unknown ids are simply absent from the result; order is
    /// unspecified.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_principals(&self, ids: &[PrincipalId]) -> Result<Vec<Principal>> {
        const IDS_PER_STATEMENT: usize = 32_000;
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql =
                format!("SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE id IN ({placeholders})");
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            let rows = query
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("get principals failed: {e}")))?;
            out.extend(rows.iter().map(map_principal_row));
        }
        Ok(out)
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

    /// The principals an unstamped (pre-multiplayer) user message in
    /// `workspace_id` resolves to at serve time, in fallback order:
    /// `legacy_author_principal_id`, then `owner_principal_id`. `None` when
    /// the workspace does not exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_author_fallback(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceAuthorFallback>> {
        let row = sqlx::query(
            "SELECT legacy_author_principal_id, owner_principal_id FROM workspace WHERE id = ?",
        )
        .bind(&workspace_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get workspace author fallback failed: {e}")))?;
        Ok(row.map(|r| WorkspaceAuthorFallback {
            legacy_author_principal_id: r
                .get::<Option<String>, _>("legacy_author_principal_id")
                .map(PrincipalId),
            owner_principal_id: r
                .get::<Option<String>, _>("owner_principal_id")
                .map(PrincipalId),
        }))
    }

    /// Set (or clear) the workspace's `legacy_author_principal_id` — the
    /// principal its unstamped user messages are credited to.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when the workspace does not exist and
    /// `Error::Internal` if the database operation fails.
    pub async fn set_workspace_legacy_author_principal_id(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: Option<&PrincipalId>,
    ) -> Result<()> {
        let result =
            sqlx::query("UPDATE workspace SET legacy_author_principal_id = ? WHERE id = ?")
                .bind(principal_id.map(|p| p.0.as_str()))
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("set workspace legacy author failed: {e}")))?;
        if result.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {workspace_id}")));
        }
        Ok(())
    }

    /// Membership summaries for `workspace.get` / `workspace.list`
    /// (multiplayer w1): owner, member count and `viewer`'s role, computed
    /// in SQL in ONE query scoped to exactly `workspace_ids` (the rows the
    /// caller is about to return — never the whole table, so archived or
    /// filtered-out workspaces cost nothing), keyed by workspace id. An empty
    /// `workspace_ids` short-circuits without touching the database.
    /// `viewer = None` yields no `my_role`. `open_invite_count` is `0` until
    /// invitations exist.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn workspace_membership_summaries(
        &self,
        viewer: Option<&PrincipalId>,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, WorkspaceMembership>> {
        if workspace_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = vec!["?"; workspace_ids.len()].join(",");
        let sql = format!(
            "SELECT w.id AS workspace_id, w.owner_principal_id, \
                (SELECT COUNT(*) FROM workspace_member m WHERE m.workspace_id = w.id) AS member_count, \
                (SELECT COUNT(*) FROM workspace_invite i WHERE i.workspace_id = w.id \
                    AND i.redeemed_at IS NULL AND i.revoked_at IS NULL \
                    AND i.expires_at > ?) AS open_invite_count, \
                (SELECT m.role FROM workspace_member m \
                    WHERE m.workspace_id = w.id AND m.principal_id = ?) AS my_role \
             FROM workspace w WHERE w.id IN ({placeholders})"
        );
        let mut query = sqlx::query(&sql)
            .bind(now_iso())
            .bind(viewer.map(|p| p.0.as_str()));
        for id in workspace_ids {
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
                        open_invite_count: u64::try_from(r.get::<i64, _>("open_invite_count"))
                            .unwrap_or(0),
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

    /// A principal's role in a workspace; `None` when not a member.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_member_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<Option<WorkspaceRole>> {
        let row = sqlx::query(
            "SELECT role FROM workspace_member WHERE workspace_id = ? AND principal_id = ?",
        )
        .bind(&workspace_id.0)
        .bind(&principal_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get workspace member role failed: {e}")))?;
        row.map(|r| enum_from_db::<WorkspaceRole>(&r.get::<String, _>("role")))
            .transpose()
    }

    /// Add a principal to a workspace with `role`. Idempotent: an existing
    /// membership is left untouched (use
    /// [`Store::set_workspace_member_role`] to change its role). Returns
    /// whether a row was inserted. `workspace.owner_principal_id` is
    /// re-derived from the owner membership in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidInput` when `role` is `Owner` and the workspace
    /// already has one (exactly one owner per workspace, migration `0126`)
    /// and `Error::Internal` if the database operation fails (including an
    /// unknown workspace or principal, rejected by the FKs).
    pub async fn add_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<bool> {
        let pool = self.write_pool();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("add workspace member begin failed: {e}")))?;
            let sql = format!(
                "INSERT INTO workspace_member ({MEMBER_COLUMNS}) VALUES (?,?,?,?) \
                 ON CONFLICT(workspace_id, principal_id) DO NOTHING"
            );
            let res = sqlx::query(&sql)
                .bind(&workspace_id.0)
                .bind(&principal_id.0)
                .bind(role.as_str())
                .bind(now_iso())
                .execute(&mut *tx)
                .await
                .map_err(|e| map_owner_violation(&e, workspace_id, "add workspace member"))?;
            sync_workspace_owner(&mut tx, workspace_id).await?;
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("add workspace member commit failed: {e}")))?;
            Ok(res.rows_affected() > 0)
        })
        .await
    }

    /// Change an existing member's role. `workspace.owner_principal_id` is
    /// re-derived from the owner membership in the same transaction, so
    /// demoting the owner clears it and promoting a member sets it.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when the principal is not a member of the
    /// workspace, `Error::InvalidInput` when promoting to `Owner` while the
    /// workspace already has one, and `Error::Internal` if the database
    /// operation fails.
    pub async fn set_workspace_member_role(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        role: WorkspaceRole,
    ) -> Result<()> {
        let pool = self.write_pool();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("set workspace member role begin failed: {e}"))
            })?;
            let res = sqlx::query(
                "UPDATE workspace_member SET role = ? WHERE workspace_id = ? AND principal_id = ?",
            )
            .bind(role.as_str())
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_owner_violation(&e, workspace_id, "set workspace member role"))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!(
                    "principal {principal_id} is not a member of workspace {workspace_id}"
                )));
            }
            sync_workspace_owner(&mut tx, workspace_id).await?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("set workspace member role commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
    }

    /// Remove a principal from a workspace. Returns whether a row was
    /// removed; removing a non-member is not an error. Removing the owner
    /// clears `workspace.owner_principal_id` in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn remove_workspace_member(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let pool = self.write_pool();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("remove workspace member begin failed: {e}"))
            })?;
            let res = sqlx::query(
                "DELETE FROM workspace_member WHERE workspace_id = ? AND principal_id = ?",
            )
            .bind(&workspace_id.0)
            .bind(&principal_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("remove workspace member failed: {e}")))?;
            sync_workspace_owner(&mut tx, workspace_id).await?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("remove workspace member commit failed: {e}"))
            })?;
            Ok(res.rows_affected() > 0)
        })
        .await
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

    /// Resolve an **active** credential to its principal and bump
    /// `last_used_at` in one statement. `None` for an unknown or revoked
    /// hash. The single `UPDATE … WHERE revoked_at IS NULL RETURNING` closes
    /// the lookup-then-touch window in which a concurrent revoke would
    /// otherwise still admit the credential (intent-hq/intentd#1868).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn resolve_active_principal_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<PrincipalId>> {
        let row = sqlx::query(
            "UPDATE principal_credential SET last_used_at = ? \
             WHERE token_hash = ? AND revoked_at IS NULL \
             RETURNING principal_id",
        )
        .bind(now_iso())
        .bind(token_hash)
        .fetch_optional(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("resolve principal credential failed: {e}")))?;
        Ok(row.map(|r| PrincipalId(r.get::<String, _>("principal_id"))))
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

    /// Revoke every active credential of a principal. Returns how many rows
    /// flipped from active to revoked.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn revoke_all_principal_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE principal_credential SET revoked_at = ? \
             WHERE principal_id = ? AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(&principal_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("revoke principal credentials failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Number of `principal` rows (primary included). Used by the primary
    /// identity reconnect guard (multiplayer w4).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn count_principals(&self) -> Result<u64> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM principal")
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("count principals failed: {e}")))?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    /// Number of open invites (not redeemed, not revoked, not expired)
    /// across every workspace (multiplayer w4).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn count_open_workspace_invites(&self) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workspace_invite \
             WHERE redeemed_at IS NULL AND revoked_at IS NULL AND expires_at > ?",
        )
        .bind(now_iso())
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("count open workspace invites failed: {e}")))?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    /// Persist a freshly minted invite (multiplayer w4). `secret_hash` is the
    /// hex SHA-256 of the link secret — the service layer hashes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_workspace_invite(&self, invite: &WorkspaceInvite) -> Result<()> {
        let sql = format!(
            "INSERT INTO workspace_invite ({INVITE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&invite.id)
            .bind(&invite.workspace_id.0)
            .bind(&invite.secret_hash)
            .bind(&invite.created_by_principal_id.0)
            .bind(invite.pin_github_user_id)
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
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert workspace invite failed: {e}")))?;
        Ok(())
    }

    /// Fetch an invite by id, open or closed (`None` when unknown).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_invite(&self, id: &str) -> Result<Option<WorkspaceInvite>> {
        let sql = format!("SELECT {INVITE_COLUMNS} FROM workspace_invite WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace invite failed: {e}")))?;
        Ok(row.as_ref().map(map_invite_row))
    }

    /// List a workspace's open invites (not redeemed, not revoked, not
    /// expired), oldest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_open_workspace_invites(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceInvite>> {
        let sql = format!(
            "SELECT {INVITE_COLUMNS} FROM workspace_invite \
             WHERE workspace_id = ? AND redeemed_at IS NULL AND revoked_at IS NULL \
               AND expires_at > ? \
             ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(now_iso())
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspace invites failed: {e}")))?;
        Ok(rows.iter().map(map_invite_row).collect())
    }

    /// Revoke an invite. Idempotent: returns whether the row flipped from
    /// open to revoked (`false` when unknown, redeemed or already revoked).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn revoke_workspace_invite(&self, id: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workspace_invite SET revoked_at = ? \
             WHERE id = ? AND redeemed_at IS NULL AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("revoke workspace invite failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Mark an **open** invite redeemed by `principal_id`. The single
    /// conditional `UPDATE` is the single-use guard: it returns `false` when
    /// the invite was redeemed, revoked or expired meanwhile, so two
    /// concurrent redemptions cannot both succeed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn redeem_workspace_invite(
        &self,
        id: &str,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let now = now_iso();
        let res = sqlx::query(
            "UPDATE workspace_invite SET redeemed_at = ?, redeemed_by_principal_id = ? \
             WHERE id = ? AND redeemed_at IS NULL AND revoked_at IS NULL AND expires_at > ?",
        )
        .bind(&now)
        .bind(&principal_id.0)
        .bind(id)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("redeem workspace invite failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

fn map_invite_row(r: &SqliteRow) -> WorkspaceInvite {
    WorkspaceInvite {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        secret_hash: r.get("secret_hash"),
        created_by_principal_id: PrincipalId(r.get("created_by_principal_id")),
        pin_github_user_id: r.get("pin_github_user_id"),
        pin_login: r.get("pin_login"),
        created_at: r.get("created_at"),
        expires_at: r.get("expires_at"),
        redeemed_at: r.get("redeemed_at"),
        redeemed_by_principal_id: r
            .get::<Option<String>, _>("redeemed_by_principal_id")
            .map(PrincipalId),
        revoked_at: r.get("revoked_at"),
    }
}

/// Re-derive `workspace.owner_principal_id` from the `owner` membership rows
/// so the column stays a faithful mirror of the membership table after every
/// membership write: the current value is kept while it still names an
/// owner; otherwise the earliest-added owner wins, and `NULL` when there is
/// none. Keeping the current owner matters because `added_at` mixes the
/// migration trigger's millisecond stamps with `now_iso()`'s nanosecond ones,
/// so ordering two rows written in the same millisecond is not meaningful.
/// Runs inside the caller's write transaction.
async fn sync_workspace_owner(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workspace_id: &WorkspaceId,
) -> Result<()> {
    sqlx::query(
        "UPDATE workspace SET owner_principal_id = COALESCE(\
            (SELECT m.principal_id FROM workspace_member m \
             WHERE m.workspace_id = workspace.id AND m.role = 'owner' \
               AND m.principal_id = workspace.owner_principal_id), \
            (SELECT m.principal_id FROM workspace_member m \
             WHERE m.workspace_id = workspace.id AND m.role = 'owner' \
             ORDER BY m.added_at, m.principal_id LIMIT 1)) \
         WHERE id = ?",
    )
    .bind(&workspace_id.0)
    .execute(&mut **tx)
    .await
    .map_err(|e| Error::Internal(format!("sync workspace owner failed: {e}")))?;
    Ok(())
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

/// Map a membership write failure: a UNIQUE violation is the one-owner index
/// (`workspace_member_owner_uq`, migration `0126`) — the `(workspace_id,
/// principal_id)` primary key is handled by `ON CONFLICT` / the `UPDATE`
/// shape and never reaches here — and surfaces as a client-facing
/// `InvalidInput`; anything else is `Internal`.
fn map_owner_violation(e: &sqlx::Error, workspace_id: &WorkspaceId, what: &str) -> Error {
    if e.as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        Error::InvalidInput(format!(
            "workspace {workspace_id} already has an owner; exactly one owner per workspace"
        ))
    } else {
        Error::Internal(format!("{what} failed: {e}"))
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
