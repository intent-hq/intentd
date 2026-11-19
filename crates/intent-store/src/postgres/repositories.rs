//! Repository lifecycle, repository authority snapshots, known repos, saved
//! repository selections and workspace transfer rows (Postgres). Mirrors
//! `repository_lifecycle.rs` (and its `initialization.rs`),
//! `repository_authority_repo.rs`, `known_repo_repo.rs`,
//! `repository_selection_repo.rs` and `transfer_repo.rs`.
//!
//! Lifecycle: every open of one tenant in one database shares one lifecycle
//! domain, as managed opens of one `SQLite` file do. Installing an observer
//! is refused, the way `SQLite` refuses it without managed file identity:
//! the Postgres writers in `workspaces.rs`, `agents.rs`, `principals.rs` and
//! `agent_messages.rs` leave out the lifecycle brackets, so an observer would
//! miss their mutations. With no observer, claims and confirmations never
//! match, and the brackets kept here (pending delete, selection writes,
//! transfer import, compatible ACP writes) only serialize and track their
//! owners, as `SQLite`'s do before an observer is installed.
//!
//! Multi-statement reads that `SQLite` runs in one WAL snapshot run in one
//! `REPEATABLE READ` transaction. Text ordering uses `COLLATE "C"` to match
//! `SQLite`'s byte order, with `row_seq` (insertion order) breaking ties.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use intent_core::transfer::TransferTableStat;
use intent_core::{
    now_iso, AgentId, Error, KnownRepo, Principal, PrincipalId, RepositoryRootId,
    RepositoryRootKind, Result, TokenUsageTotals, UsageCost, WorkspaceId,
};
use sqlx::postgres::{PgConnection, PgRow, Postgres};
use sqlx::{Column, Row, Transaction, TypeInfo, ValueRef};

use super::{db, PgStore, WriteTx};
use crate::enum_from_db;
use crate::repository_authority_repo::{
    counter, AuthorityRevision, RepositoryAuthoritySnapshot, RepositoryCredentialAuthority,
    RepositoryHostAuthoritySnapshot, RepositoryPrincipalAuthority, RepositoryWorkspaceAuthority,
    RepositoryWorkspaceAuthoritySnapshot, VersionedAuthority,
};
use crate::repository_lifecycle::initialization::{observation, CompatibilityBody};
use crate::repository_lifecycle::{lifecycle_error, LifecycleDomain, LifecycleWrite};
use crate::repository_selection_repo::{
    self as selection, RepositorySelectionBinding, RepositorySelectionChange,
    RepositorySelectionPersistence, RepositorySelectionRevision, RepositorySelectionSnapshot,
    RepositorySelectionWriteOutcome, RepositorySelectionWriteResult, SelectionAdmission,
    SelectionComparison, StoredSelectionRow,
};
use crate::transfer_repo::{base64_decode, base64_encode, TRANSFER_TABLES};
use crate::{
    RepositoryAcpCompatibilityEffect, RepositoryAcpCompatibilityOutcome,
    RepositoryAcpCompatibilityPersistence, RepositoryAcpCompatibilityResult,
    RepositoryAcpInitialization, RepositoryInitializationBinding, RepositoryInitializationClaim,
    RepositoryInitializationConfirmation, RepositoryInitializationObservation,
    RepositoryInitializationOutcome, RepositoryInitializationPersistence, RepositoryLifecycleKey,
    RepositoryLifecycleObserver, RepositoryPendingDeleteGuard,
};

impl PgStore {
    /// A read-only transaction with one snapshot for all its statements.
    async fn begin_repository_snapshot(&self) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .await
            .map_err(db)?;
        if self.options.rls_guard {
            sqlx::query("SELECT set_config('intent.tenant_id', $1, true)")
                .bind(self.tenant())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        Ok(tx)
    }
}

// ---- repository lifecycle ----------------------------------------------------

fn session_of(binding: &RepositoryInitializationBinding) -> &String {
    match &binding.action {
        RepositoryAcpInitialization::FirstSet { session_id }
        | RepositoryAcpInitialization::Loaded { session_id }
        | RepositoryAcpInitialization::Replace { session_id, .. } => session_id,
    }
}

impl PgStore {
    pub(crate) fn repository_lifecycle_domain(&self) -> Result<Arc<LifecycleDomain>> {
        let options = self.pool.connect_options();
        crate::repository_lifecycle::postgres_domain(format!(
            "{}:{}/{:?}/{:?}#{}",
            options.get_host(),
            options.get_port(),
            options.get_database(),
            options.get_socket(),
            self.tenant()
        ))
    }

    pub(super) async fn repository_lifecycle_write(&self) -> Result<LifecycleWrite> {
        self.repository_lifecycle_domain()?.write().await
    }

    pub async fn begin_repository_pending_delete(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<RepositoryPendingDeleteGuard> {
        self.repository_lifecycle_domain()?
            .begin_pending_delete(keys)
            .await
    }

    #[expect(clippy::unused_async, reason = "same signature as SqliteStore")]
    pub async fn install_repository_lifecycle_observer(
        &self,
        observer: Arc<dyn RepositoryLifecycleObserver>,
    ) -> Result<()> {
        debug_assert!(!self.has_repository_lifecycle_observer(&observer));
        Err(lifecycle_error(
            "Postgres writers do not report repository mutations",
        ))
    }

    pub fn has_repository_lifecycle_observer(
        &self,
        observer: &Arc<dyn RepositoryLifecycleObserver>,
    ) -> bool {
        self.repository_lifecycle_domain()
            .is_ok_and(|domain| domain.has_observer(observer))
    }

    pub fn bind_repository_initialization_claim(
        &self,
        observer: Arc<dyn RepositoryLifecycleObserver>,
        original_owner: Box<dyn Any + Send>,
    ) -> Result<RepositoryInitializationClaim> {
        if !self.has_repository_lifecycle_observer(&observer) {
            return Err(lifecycle_error("original observer is unavailable"));
        }
        Ok(RepositoryInitializationClaim {
            domain: self.repository_lifecycle_domain()?,
            observer,
            original_owner,
        })
    }

    /// Whether `claim` belongs to this store's domain and its installed
    /// observer. Never on Postgres, which installs no observer.
    fn claim_matches(&self, claim: &RepositoryInitializationClaim) -> bool {
        self.repository_lifecycle_domain()
            .is_ok_and(|domain| Arc::ptr_eq(&claim.domain, &domain))
            && self.has_repository_lifecycle_observer(&claim.observer)
    }

    pub async fn initialize_repository_acp_session(
        &self,
        claim: RepositoryInitializationClaim,
        binding: RepositoryInitializationBinding,
    ) -> Result<RepositoryInitializationConfirmation> {
        self.initialize_repository_acp_session_outcome(claim, binding)
            .await
            .confirmation
    }

    /// `SQLite` rejects a claim that does not match before any SQL; no claim
    /// matches here, so the strict path ends there.
    #[expect(clippy::unused_async, reason = "same signature as SqliteStore")]
    pub async fn initialize_repository_acp_session_outcome(
        &self,
        claim: RepositoryInitializationClaim,
        _binding: RepositoryInitializationBinding,
    ) -> RepositoryInitializationOutcome {
        debug_assert!(!self.claim_matches(&claim));
        RepositoryInitializationOutcome {
            persistence: RepositoryInitializationPersistence::NotAttempted,
            confirmation: Err(lifecycle_error("initialization domain/observer changed")),
        }
    }

    pub async fn initialize_repository_review_session_outcome(
        &self,
        claim: RepositoryInitializationClaim,
        binding: RepositoryInitializationBinding,
        _enrollment: crate::ReviewContextEnrollmentIntent,
    ) -> RepositoryInitializationOutcome {
        self.initialize_repository_acp_session_outcome(claim, binding)
            .await
    }

    pub fn consume_repository_initialization_confirmation(
        &self,
        observer: &Arc<dyn RepositoryLifecycleObserver>,
        _confirmation: RepositoryInitializationConfirmation,
    ) -> Result<(RepositoryInitializationBinding, Box<dyn Any + Send>)> {
        // `SQLite` also requires the observer to be installed here, and no
        // observer ever is.
        debug_assert!(!self.has_repository_lifecycle_observer(observer));
        Err(lifecycle_error("confirmation domain/observer changed"))
    }

    pub async fn initialize_repository_acp_session_compatible(
        &self,
        claim: Option<RepositoryInitializationClaim>,
        binding: RepositoryInitializationBinding,
    ) -> RepositoryAcpCompatibilityOutcome {
        let mut persistence = RepositoryAcpCompatibilityPersistence::NotAttempted;
        let mut confirmation = Err(lifecycle_error(
            "original initialization proof is unavailable",
        ));
        let result = match claim {
            Some(claim) => {
                debug_assert!(!self.claim_matches(&claim));
                Err(lifecycle_error("initialization domain/observer changed"))
            }
            None => {
                self.compatible_acp_write(&binding, &mut persistence, &mut confirmation)
                    .await
            }
        };
        RepositoryAcpCompatibilityOutcome {
            persistence,
            result,
            confirmation,
        }
    }

    /// `initialize_repository_acp_session_compatible_inner` without a claim:
    /// the ordinary ACP write, with no observer ticket to settle.
    async fn compatible_acp_write(
        &self,
        binding: &RepositoryInitializationBinding,
        persistence: &mut RepositoryAcpCompatibilityPersistence,
        confirmation: &mut Result<RepositoryInitializationConfirmation>,
    ) -> Result<RepositoryAcpCompatibilityResult> {
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut tx = self.begin_write().await?;
        *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: RepositoryInitializationObservation::NotRead,
        };
        let body = self
            .compatible_acp_body(&mut tx, binding, &mut lifecycle, persistence)
            .await;
        let committed = match body {
            Ok(body) => {
                tx.commit().await.map_err(|e| {
                    Error::Internal(format!("compatibility initialization commit failed: {e}"))
                })?;
                body
            }
            Err(e) => {
                let _ = rollback(tx).await;
                return Err(e);
            }
        };
        if committed.dispatched {
            *persistence = RepositoryAcpCompatibilityPersistence::Committed {
                observed: committed.observed.clone(),
                affected_rows: committed.rows,
            };
        }
        lifecycle.release_serialization();
        if let Err(e) = lifecycle.settle_after_retirement().await {
            *confirmation = Err(lifecycle_error(
                "original initialization completion unavailable",
            ));
            return Err(e);
        }
        Ok(committed.into_result())
    }

    async fn compatible_acp_body(
        &self,
        tx: &mut WriteTx,
        binding: &RepositoryInitializationBinding,
        lifecycle: &mut LifecycleWrite,
        persistence: &mut RepositoryAcpCompatibilityPersistence,
    ) -> Result<CompatibilityBody> {
        let session_id = session_of(binding);
        let before = self.compatibility_read(tx, binding).await?;
        *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
            observed: observation(before.as_ref()),
        };
        if matches!(binding.action, RepositoryAcpInitialization::Loaded { .. }) {
            let matches_loaded = before.as_ref().and_then(Option::as_ref) == Some(session_id);
            return Ok(CompatibilityBody {
                canonical: session_id.clone(),
                observed: observation(before.as_ref()),
                effect: None,
                rows: 0,
                dispatched: false,
                strict_winner: matches_loaded && !session_id.is_empty(),
            });
        }
        let stored = before
            .clone()
            .ok_or_else(|| Error::NotFound(format!("agent session {}", binding.agent_id)))?;
        let effect = match &binding.action {
            RepositoryAcpInitialization::FirstSet { .. } => {
                if let Some(existing) = stored.as_ref() {
                    if existing != session_id {
                        return Err(Error::Internal("acpSessionId is write-once".into()));
                    }
                    return Ok(CompatibilityBody::observed(
                        existing.clone(),
                        observation(before.as_ref()),
                    ));
                }
                RepositoryAcpCompatibilityEffect::FirstSet
            }
            RepositoryAcpInitialization::Replace { expected, .. } => {
                if let Some(existing) = stored.as_ref() {
                    if Some(existing) != expected.as_ref() {
                        return Ok(CompatibilityBody::observed(
                            existing.clone(),
                            observation(before.as_ref()),
                        ));
                    }
                }
                if stored.as_ref() == Some(session_id) {
                    RepositoryAcpCompatibilityEffect::AccountingOnly
                } else {
                    RepositoryAcpCompatibilityEffect::Replace {
                        previous: stored.clone(),
                    }
                }
            }
            RepositoryAcpInitialization::Loaded { .. } => unreachable!(),
        };
        let strict_eligible = !session_id.is_empty()
            && match &binding.action {
                RepositoryAcpInitialization::FirstSet { .. } => stored.is_none(),
                RepositoryAcpInitialization::Replace { expected, .. } => {
                    stored == *expected && stored.as_ref() != Some(session_id)
                }
                RepositoryAcpInitialization::Loaded { .. } => false,
            };
        if stored.as_ref() != Some(session_id) {
            lifecycle.begin(&[RepositoryLifecycleKey::Agent(binding.agent_id.clone())])?;
        }
        lifecycle.mark_begun();
        *persistence = RepositoryAcpCompatibilityPersistence::Unknown;
        let (canonical, rows, dispatched) =
            if matches!(binding.action, RepositoryAcpInitialization::FirstSet { .. }) {
                let rows = sqlx::query(
                    "UPDATE agent_session SET acp_session_id = $4 \
                     WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
                )
                .bind(self.tenant())
                .bind(&binding.agent_id.0)
                .bind(&binding.workspace_id.0)
                .bind(session_id)
                .execute(&mut **tx)
                .await
                .map_err(|e| lifecycle_error(&format!("set acp session id failed: {e}")))?
                .rows_affected();
                (session_id.clone(), rows, true)
            } else {
                self.write_acp_session_id(
                    tx,
                    &binding.workspace_id,
                    &binding.agent_id,
                    stored.as_deref(),
                    session_id,
                )
                .await?
            };
        let actual = self.compatibility_read(tx, binding).await?;
        if !dispatched {
            *persistence = RepositoryAcpCompatibilityPersistence::NoEffect {
                observed: observation(actual.as_ref()),
            };
        }
        Ok(CompatibilityBody {
            canonical,
            strict_winner: strict_eligible
                && rows == 1
                && actual.as_ref().and_then(Option::as_ref) == Some(session_id),
            observed: observation(actual.as_ref()),
            effect: Some(effect),
            rows,
            dispatched,
        })
    }

    async fn compatibility_read(
        &self,
        conn: &mut PgConnection,
        binding: &RepositoryInitializationBinding,
    ) -> Result<Option<Option<String>>> {
        sqlx::query_scalar(
            "SELECT acp_session_id FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&binding.agent_id.0)
        .bind(&binding.workspace_id.0)
        .fetch_optional(conn)
        .await
        .map_err(|e| lifecycle_error(&format!("compatibility initialization read failed: {e}")))
    }

    /// `SqliteStore::write_acp_session_id_in_transaction`: `(canonical,
    /// rows_affected, statement_dispatched)`.
    async fn write_acp_session_id(
        &self,
        tx: &mut WriteTx,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_old: Option<&str>,
        acp_session_id: &str,
    ) -> Result<(String, u64, bool)> {
        let row: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT acp_session_id, token_usage, token_usage_baseline FROM agent_session \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("replace acp session id read failed: {e}")))?;
        let stored_id = row.as_ref().and_then(|r| r.0.clone());
        if stored_id.as_deref() != expected_old {
            return Ok((
                stored_id.unwrap_or_else(|| acp_session_id.to_string()),
                0,
                false,
            ));
        }
        let decode = |raw: Option<String>| -> Option<TokenUsageTotals> {
            raw.and_then(|s| serde_json::from_str(&s).ok())
        };
        let (snapshot, baseline) = row.map_or((None, None), |r| (decode(r.1), decode(r.2)));
        let folded = match (&baseline, &snapshot) {
            (None, None) => None,
            (b, s) => {
                let b = b.clone().unwrap_or_default();
                let s = s.clone().unwrap_or_default();
                Some(TokenUsageTotals {
                    input_tokens: b.input_tokens.saturating_add(s.input_tokens),
                    output_tokens: b.output_tokens.saturating_add(s.output_tokens),
                    cache_read_tokens: b.cache_read_tokens.saturating_add(s.cache_read_tokens),
                    cache_creation_tokens: b
                        .cache_creation_tokens
                        .saturating_add(s.cache_creation_tokens),
                    thought_tokens: b.thought_tokens.saturating_add(s.thought_tokens),
                    cost: UsageCost::merge(b.cost.as_ref(), s.cost.as_ref()),
                })
            }
        };
        let folded_json = folded
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("encode token_usage_baseline failed: {e}")))?;
        let rows_affected = sqlx::query(
            "UPDATE agent_session SET acp_session_id = $4, token_usage_baseline = $5, \
             token_usage = NULL WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(acp_session_id)
        .bind(folded_json)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("replace acp session id failed: {e}")))?
        .rows_affected();
        Ok((acp_session_id.to_string(), rows_affected, true))
    }
}

async fn rollback(tx: WriteTx) -> std::result::Result<(), sqlx::Error> {
    tx.rollback().await
}

// ---- repository authority ----------------------------------------------------

#[expect(
    clippy::needless_pass_by_value,
    reason = "Result::map_err passes its owned database error"
)]
fn authority_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("repository authority snapshot failed: {error}"))
}

impl PgStore {
    async fn versioned<T>(
        &self,
        conn: &mut PgConnection,
        kind: &str,
        key: &str,
        member: &str,
        value: Option<T>,
    ) -> Result<VersionedAuthority<T>> {
        let raw: Option<i64> = sqlx::query_scalar(
            "SELECT revision FROM repository_authority_revision \
             WHERE tenant_id = $1 AND kind = $2 AND subject_id = $3 AND member_id = $4",
        )
        .bind(self.tenant())
        .bind(kind)
        .bind(key)
        .bind(member)
        .fetch_optional(conn)
        .await
        .map_err(authority_error)?;
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

    async fn authority_principal(
        &self,
        conn: &mut PgConnection,
        id: &PrincipalId,
    ) -> Result<VersionedAuthority<RepositoryPrincipalAuthority>> {
        let row = sqlx::query(
            "SELECT id, is_primary, github_user_id, identity_provider, instance_host, \
             external_user_id FROM principal WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(authority_error)?;
        let value = row
            .map(|row| -> Result<RepositoryPrincipalAuthority> {
                Ok(RepositoryPrincipalAuthority {
                    id: PrincipalId(row.try_get("id").map_err(authority_error)?),
                    is_primary: row
                        .try_get::<i64, _>("is_primary")
                        .map_err(authority_error)?
                        != 0,
                    github_user_id: row.try_get("github_user_id").map_err(authority_error)?,
                    identity_provider: row.try_get("identity_provider").map_err(authority_error)?,
                    instance_host: row.try_get("instance_host").map_err(authority_error)?,
                    external_user_id: row.try_get("external_user_id").map_err(authority_error)?,
                })
            })
            .transpose()?;
        self.versioned(conn, "principal", id.as_str(), "", value)
            .await
    }

    async fn primary_authority(
        &self,
        conn: &mut PgConnection,
    ) -> Result<VersionedAuthority<RepositoryPrincipalAuthority>> {
        let primary_id: Option<String> =
            sqlx::query_scalar("SELECT id FROM principal WHERE tenant_id = $1 AND is_primary = 1")
                .bind(self.tenant())
                .fetch_optional(&mut *conn)
                .await
                .map_err(authority_error)?;
        match primary_id {
            Some(id) => self.authority_principal(conn, &PrincipalId(id)).await,
            None => Ok(VersionedAuthority {
                revision: None,
                value: None,
            }),
        }
    }

    async fn host_member_authority(
        &self,
        conn: &mut PgConnection,
        principal_id: &PrincipalId,
    ) -> Result<VersionedAuthority<()>> {
        let host: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM host_member WHERE tenant_id = $1 AND principal_id = $2",
        )
        .bind(self.tenant())
        .bind(principal_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(authority_error)?;
        self.versioned(
            conn,
            "host_member",
            principal_id.as_str(),
            "",
            host.map(|_| ()),
        )
        .await
    }

    async fn credential_authority(
        &self,
        conn: &mut PgConnection,
        original_token_hash: Option<&str>,
    ) -> Result<Option<VersionedAuthority<RepositoryCredentialAuthority>>> {
        let Some(hash) = original_token_hash else {
            return Ok(None);
        };
        let row: Option<(String, bool)> = sqlx::query_as(
            "SELECT principal_id, revoked_at IS NOT NULL AS revoked FROM principal_credential \
             WHERE tenant_id = $1 AND token_hash = $2",
        )
        .bind(self.tenant())
        .bind(hash)
        .fetch_optional(&mut *conn)
        .await
        .map_err(authority_error)?;
        let value = row.map(|(principal_id, revoked)| RepositoryCredentialAuthority {
            principal_id: PrincipalId(principal_id),
            revoked,
        });
        Ok(Some(
            self.versioned(conn, "credential", hash, "", value).await?,
        ))
    }

    async fn host_generation(&self, conn: &mut PgConnection) -> Result<u64> {
        let raw: i64 = sqlx::query_scalar(
            "SELECT authorization_generation FROM host_membership_state \
             WHERE tenant_id = $1 AND id = 1",
        )
        .bind(self.tenant())
        .fetch_one(conn)
        .await
        .map_err(authority_error)?;
        counter(raw, false)
    }

    async fn revocation_generation(
        &self,
        conn: &mut PgConnection,
        principal_id: &PrincipalId,
    ) -> Result<Option<u64>> {
        let raw: Option<i64> = sqlx::query_scalar(
            "SELECT generation FROM principal_revocation WHERE tenant_id = $1 AND principal_id = $2",
        )
        .bind(self.tenant())
        .bind(principal_id.as_str())
        .fetch_optional(conn)
        .await
        .map_err(authority_error)?;
        raw.map(|n| counter(n, true)).transpose()
    }

    async fn workspace_authority(
        &self,
        conn: &mut PgConnection,
        workspace_id: &WorkspaceId,
    ) -> Result<VersionedAuthority<RepositoryWorkspaceAuthority>> {
        let owner: Option<Option<String>> = sqlx::query_scalar(
            "SELECT owner_principal_id FROM workspace WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(authority_error)?;
        let value = owner.map(|owner| RepositoryWorkspaceAuthority {
            owner_principal_id: owner.map(PrincipalId),
        });
        self.versioned(conn, "workspace", workspace_id.as_str(), "", value)
            .await
    }

    /// `repository_authority_repo::read_snapshot`.
    async fn read_authority_snapshot(
        &self,
        conn: &mut PgConnection,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        original_token_hash: Option<&str>,
    ) -> Result<RepositoryAuthoritySnapshot> {
        let workspace = self.workspace_authority(conn, workspace_id).await?;
        let person = self.authority_principal(conn, principal_id).await?;
        let primary_principal = self.primary_authority(conn).await?;
        let host_member = self.host_member_authority(conn, principal_id).await?;
        let role: Option<String> = sqlx::query_scalar(
            "SELECT role FROM workspace_member \
             WHERE tenant_id = $1 AND workspace_id = $2 AND principal_id = $3",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(principal_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(authority_error)?;
        let workspace_grant = self
            .versioned(
                conn,
                "workspace_member",
                workspace_id.as_str(),
                principal_id.as_str(),
                role.as_deref().map(enum_from_db).transpose()?,
            )
            .await?;
        let credential = self.credential_authority(conn, original_token_hash).await?;
        let host_authorization_generation = self.host_generation(conn).await?;
        let principal_revocation_generation =
            self.revocation_generation(conn, principal_id).await?;
        Ok(RepositoryAuthoritySnapshot {
            workspace_id: workspace_id.clone(),
            principal_id: principal_id.clone(),
            workspace,
            principal: person,
            primary_principal,
            host_member,
            workspace_grant,
            credential,
            host_authorization_generation,
            principal_revocation_generation,
        })
    }

    pub async fn repository_host_authority_snapshot(
        &self,
        principal_id: &PrincipalId,
        original_token_hash: Option<&str>,
    ) -> Result<RepositoryHostAuthoritySnapshot> {
        let mut tx = self.begin_repository_snapshot().await?;
        let snapshot = self
            .read_host_authority_snapshot(&mut tx, principal_id, original_token_hash)
            .await?;
        tx.commit().await.map_err(authority_error)?;
        Ok(snapshot)
    }

    /// The host authority as `conn` sees it. Inside a writer transaction
    /// no revocation can commit until that transaction ends.
    pub(super) async fn read_host_authority_snapshot(
        &self,
        conn: &mut PgConnection,
        principal_id: &PrincipalId,
        original_token_hash: Option<&str>,
    ) -> Result<RepositoryHostAuthoritySnapshot> {
        let person = self.authority_principal(conn, principal_id).await?;
        let primary_principal = self.primary_authority(conn).await?;
        let host_member = self.host_member_authority(conn, principal_id).await?;
        let credential = self.credential_authority(conn, original_token_hash).await?;
        let host_authorization_generation = self.host_generation(conn).await?;
        let principal_revocation_generation =
            self.revocation_generation(conn, principal_id).await?;
        Ok(RepositoryHostAuthoritySnapshot {
            principal_id: principal_id.clone(),
            principal: person,
            primary_principal,
            host_member,
            credential,
            host_authorization_generation,
            principal_revocation_generation,
        })
    }

    pub async fn repository_workspace_authority_snapshot(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<RepositoryWorkspaceAuthoritySnapshot> {
        let mut tx = self.begin_repository_snapshot().await?;
        let workspace = self.workspace_authority(&mut tx, workspace_id).await?;
        let host_authorization_generation = self.host_generation(&mut tx).await?;
        tx.commit().await.map_err(authority_error)?;
        Ok(RepositoryWorkspaceAuthoritySnapshot {
            workspace_id: workspace_id.clone(),
            workspace,
            host_authorization_generation,
        })
    }

    pub async fn repository_authority_snapshot(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
        original_token_hash: Option<&str>,
    ) -> Result<RepositoryAuthoritySnapshot> {
        let mut tx = self.begin_repository_snapshot().await?;
        let snapshot = self
            .read_authority_snapshot(&mut tx, workspace_id, principal_id, original_token_hash)
            .await?;
        tx.commit().await.map_err(authority_error)?;
        Ok(snapshot)
    }
}

// ---- known repos ---------------------------------------------------------------

impl PgStore {
    pub async fn list_known_repos(&self) -> Result<Vec<KnownRepo>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String, Option<String>, String, String)> = sqlx::query_as(
            "SELECT path, name, owner, added_at, last_used_at FROM known_repo \
             WHERE tenant_id = $1 ORDER BY last_used_at COLLATE \"C\" DESC, row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("list known repos failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(path, name, owner, added_at, last_used_at)| KnownRepo {
                path,
                name,
                owner,
                added_at,
                last_used_at,
            })
            .collect())
    }

    pub async fn known_repo_exists(&self, path: &str) -> Result<bool> {
        let mut tx = self.begin().await?;
        let exists = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM known_repo WHERE tenant_id = $1 AND path = $2)",
        )
        .bind(self.tenant())
        .bind(path)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("known repo lookup failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(exists)
    }

    pub async fn upsert_known_repo(
        &self,
        path: &str,
        name: &str,
        owner: Option<&str>,
    ) -> Result<()> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO known_repo (tenant_id, path, name, owner, added_at, last_used_at) \
             VALUES ($1, $2, $3, $4, $5, $5) \
             ON CONFLICT (tenant_id, path) DO UPDATE SET \
                 name = CASE WHEN excluded.name <> '' THEN excluded.name ELSE known_repo.name END, \
                 owner = COALESCE(excluded.owner, known_repo.owner), \
                 last_used_at = excluded.last_used_at",
        )
        .bind(self.tenant())
        .bind(path)
        .bind(name)
        .bind(owner)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("upsert known repo failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn remove_known_repo(&self, path: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query("DELETE FROM known_repo WHERE tenant_id = $1 AND path = $2")
            .bind(self.tenant())
            .bind(path)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("remove known repo failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(res.rows_affected() > 0)
    }
}

// ---- repository selection --------------------------------------------------------

impl PgStore {
    /// `repository_selection_repo::read_at`.
    async fn selection_read_at(
        &self,
        domain: &Arc<LifecycleDomain>,
        root: &RepositoryRootId,
        conn: &mut PgConnection,
    ) -> Result<RepositorySelectionSnapshot> {
        let workspace: Option<(Option<String>, Option<String>, i64)> = sqlx::query_as(
            "SELECT repository_path, worktree_path, is_remote FROM workspace \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(root.workspace_id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(selection::sql_error)?;
        let binding = match (&root.kind, workspace) {
            (_, None) => None,
            (RepositoryRootKind::Primary, Some(row)) => Some(selection::primary_binding(row)?),
            (RepositoryRootKind::Registered { git_root_id }, Some(_)) => sqlx::query_scalar(
                "SELECT path FROM workspace_git_root \
                     WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
            )
            .bind(self.tenant())
            .bind(git_root_id.as_str())
            .bind(root.workspace_id.as_str())
            .fetch_optional(&mut *conn)
            .await
            .map_err(selection::sql_error)?
            .map(|path| RepositorySelectionBinding::Registered { path }),
        };
        let (kind, id) = selection::key(root);
        let state: Option<StoredSelectionRow> = sqlx::query_as(
            "SELECT * FROM repository_selection_state \
             WHERE tenant_id = $1 AND workspace_id = $2 AND root_kind = $3 AND root_id = $4",
        )
        .bind(self.tenant())
        .bind(root.workspace_id.as_str())
        .bind(kind)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(selection::sql_error)?;
        selection::assemble(domain.clone(), root, binding, state.as_ref())
    }

    /// `SelectionAdmission::current`.
    async fn selection_authority_current(
        &self,
        authority: &RepositoryAuthoritySnapshot,
        token_hash: Option<&str>,
        original: &RepositorySelectionSnapshot,
        conn: &mut PgConnection,
    ) -> Result<bool> {
        if original.root().workspace_id != authority.workspace_id {
            return Ok(false);
        }
        Ok(self
            .read_authority_snapshot(
                conn,
                &original.root().workspace_id,
                &authority.principal_id,
                token_hash,
            )
            .await?
            == *authority)
    }

    pub async fn repository_selection_snapshot(
        &self,
        root: &RepositoryRootId,
    ) -> Result<RepositorySelectionSnapshot> {
        let domain = self.repository_lifecycle_domain()?;
        let _serial = domain.write().await?;
        let mut tx = self
            .begin_repository_snapshot()
            .await
            .map_err(|_| selection::error())?;
        let snapshot = self.selection_read_at(&domain, root, &mut tx).await?;
        tx.commit().await.map_err(selection::sql_error)?;
        Ok(snapshot)
    }

    pub async fn write_repository_selection(
        &self,
        original: &RepositorySelectionSnapshot,
        change: RepositorySelectionChange,
    ) -> RepositorySelectionWriteOutcome {
        let mut persistence = RepositorySelectionPersistence::NotAttempted;
        match self
            .write_selection_inner(original, &change, &mut persistence, None)
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => RepositorySelectionWriteOutcome {
                result: Err(e),
                persistence,
            },
        }
    }

    pub async fn write_repository_selection_admitted(
        &self,
        original: &RepositorySelectionSnapshot,
        change: RepositorySelectionChange,
        expected_authority: &RepositoryAuthoritySnapshot,
        original_token_hash: Option<&str>,
        admit: impl FnOnce() -> Result<()> + Send,
    ) -> RepositorySelectionWriteOutcome {
        let mut persistence = RepositorySelectionPersistence::NotAttempted;
        let admission = SelectionAdmission {
            authority: expected_authority,
            token_hash: original_token_hash,
            admit: Some(Box::new(admit)),
        };
        match self
            .write_selection_inner(original, &change, &mut persistence, Some(admission))
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => RepositorySelectionWriteOutcome {
                result: Err(e),
                persistence,
            },
        }
    }

    pub async fn reset_repository_selection(
        &self,
        original: &RepositorySelectionSnapshot,
    ) -> RepositorySelectionWriteOutcome {
        self.write_repository_selection(original, RepositorySelectionChange::Reset)
            .await
    }

    async fn write_selection_inner(
        &self,
        original: &RepositorySelectionSnapshot,
        change: &RepositorySelectionChange,
        persistence: &mut RepositorySelectionPersistence,
        mut admission: Option<SelectionAdmission<'_>>,
    ) -> Result<RepositorySelectionWriteOutcome> {
        let domain = self.repository_lifecycle_domain()?;
        if !Arc::ptr_eq(&domain, &original.domain) {
            return Err(Error::InvalidParams(
                "repository selection snapshot belongs to another database".into(),
            ));
        }
        if let RepositorySelectionChange::ExplicitRemote { remote_name } = change {
            if !selection::valid_remote(remote_name) {
                return Err(Error::InvalidParams(
                    "invalid repository remote name".into(),
                ));
            }
        }
        let mut lifecycle = domain.write().await?;
        let mut read = self
            .begin_repository_snapshot()
            .await
            .map_err(|_| selection::error())?;
        let current = self
            .selection_read_at(&domain, original.root(), &mut read)
            .await?;
        let authority = admission.as_ref().map(|a| (a.authority, a.token_hash));
        let authority_current = match authority {
            Some((authority, token_hash)) => {
                self.selection_authority_current(authority, token_hash, original, &mut read)
                    .await?
            }
            None => true,
        };
        read.commit().await.map_err(selection::sql_error)?;
        if !authority_current {
            return Ok(selection::settled_denial(lifecycle, selection::admission_denied()).await);
        }
        if let SelectionComparison::Finished(result) =
            selection::classify(current, original, change)
        {
            return Ok(selection::settled(
                lifecycle,
                result,
                RepositorySelectionPersistence::NoEffect,
            )
            .await);
        }
        lifecycle.begin_selection_change(original.root())?;
        *persistence = RepositorySelectionPersistence::Unknown;
        lifecycle.resume_serialization().await?;
        let mut tx = self.begin_write().await.map_err(|_| selection::error())?;
        let current = self
            .selection_read_at(&domain, original.root(), &mut tx)
            .await?;
        if let Some((authority, token_hash)) = authority {
            if !self
                .selection_authority_current(authority, token_hash, original, &mut tx)
                .await?
            {
                rollback(tx).await.map_err(selection::sql_error)?;
                *persistence = RepositorySelectionPersistence::NoEffect;
                return Ok(
                    selection::settled_denial(lifecycle, selection::admission_denied()).await,
                );
            }
        }
        let current = match selection::classify(current, original, change) {
            SelectionComparison::Write(current) => current,
            SelectionComparison::Finished(result) => {
                rollback(tx).await.map_err(selection::sql_error)?;
                return Ok(selection::settled(
                    lifecycle,
                    result,
                    RepositorySelectionPersistence::NoEffect,
                )
                .await);
            }
        };
        let state = current.state.as_ref().ok_or_else(selection::error)?;
        let next = state
            .revision
            .checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(selection::error)?;
        let (mode, remote) = selection::desired(change);
        let (kind, id) = selection::key(original.root());
        if let Some(admission) = &mut admission {
            if let Err(denied) = admission.admit.take().ok_or_else(selection::error)?() {
                rollback(tx).await.map_err(selection::sql_error)?;
                *persistence = RepositorySelectionPersistence::NoEffect;
                return Ok(selection::settled_denial(lifecycle, denied).await);
            }
        }
        let incarnation = i64::try_from(state.incarnation).map_err(|_| selection::error())?;
        let revision = i64::try_from(state.revision).map_err(|_| selection::error())?;
        let changed = sqlx::query(
            "UPDATE repository_selection_state SET selection_revision = $5, \
             choice_incarnation = root_incarnation, choice_mode = $6, remote_name = $7, \
             historical_source = NULL, historical_record_id = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND root_kind = $3 AND root_id = $4 \
             AND root_incarnation = $8 AND selection_revision = $9",
        )
        .bind(self.tenant())
        .bind(original.root().workspace_id.as_str())
        .bind(kind)
        .bind(id)
        .bind(next)
        .bind(mode)
        .bind(remote)
        .bind(incarnation)
        .bind(revision)
        .execute(&mut *tx)
        .await
        .map_err(selection::sql_error)?;
        if changed.rows_affected() != 1 {
            return Err(selection::error());
        }
        let applied = self
            .selection_read_at(&domain, original.root(), &mut tx)
            .await?;
        tx.commit().await.map_err(selection::sql_error)?;
        let committed = RepositorySelectionPersistence::Committed {
            revision: RepositorySelectionRevision(
                u64::try_from(next).map_err(|_| selection::error())?,
            ),
        };
        *persistence = committed;
        Ok(selection::settled(
            lifecycle,
            RepositorySelectionWriteResult::Applied(applied),
            committed,
        )
        .await)
    }

    /// `repository_selection_repo::imported_primary_before`.
    async fn imported_primary_before(
        &self,
        tx: &mut WriteTx,
        id: &WorkspaceId,
    ) -> Result<Option<(i64, i64)>> {
        sqlx::query_as(
            "SELECT root_incarnation, selection_revision FROM repository_selection_state \
             WHERE tenant_id = $1 AND workspace_id = $2 AND root_kind = 'primary' \
             AND root_id = '' AND root_present = 1",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(selection::sql_error)
    }

    /// `repository_selection_repo::classify_imported_primary`.
    async fn classify_imported_primary(
        &self,
        tx: &mut WriteTx,
        id: &WorkspaceId,
        before: Option<(i64, i64)>,
    ) -> Result<()> {
        let after = self
            .imported_primary_before(tx, id)
            .await?
            .ok_or_else(selection::error)?;
        if before.is_some_and(|b| b.0 == after.0) {
            return Ok(());
        }
        sqlx::query(
            "UPDATE repository_selection_state SET selection_revision = selection_revision + 1, \
             choice_incarnation = root_incarnation, choice_mode = 'unresolved', \
             remote_name = NULL, historical_source = NULL, historical_record_id = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND root_kind = 'primary' \
             AND root_id = '' AND root_present = 1",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .execute(&mut **tx)
        .await
        .map_err(selection::sql_error)?;
        Ok(())
    }
}

// ---- transfer ----------------------------------------------------------------------

/// [`TRANSFER_TABLES`]' scoping predicates for Postgres, in the same order:
/// `$1` is the tenant, `$2` the workspace. Each runs as
/// `tenant_id = $1 AND (<predicate>)`.
const TRANSFER_PREDICATES: &[(&str, &str)] = &[
    ("workspace", "id = $2"),
    ("note", "workspace_id = $2"),
    ("note_version", "workspace_id = $2"),
    ("note_line_attribution", "workspace_id = $2"),
    ("comment", "workspace_id = $2"),
    ("discussion_conversation", "workspace_id = $2"),
    ("workspace_discussion_conversation", "workspace_id = $2"),
    ("discussion_participant", "workspace_id = $2"),
    ("workspace_discussion_message", "workspace_id = $2"),
    ("discussion_reaction", "workspace_id = $2"),
    ("draft", "workspace_id = $2"),
    ("agent_session", "workspace_id = $2"),
    ("script_monitor", "workspace_id = $2"),
    ("script_notification_job", "workspace_id = $2"),
    (
        "agent_queue_admission",
        "workspace_id = $2 AND phase <> 'completed'",
    ),
    (
        "agent_message",
        "agent_id IN (SELECT id FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2)",
    ),
    (
        "agent_message_payload",
        "agent_id IN (SELECT id FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2)",
    ),
    (
        "agent_usage_cell",
        "agent_id IN (SELECT id FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2)",
    ),
    (
        "agent_queue",
        "agent_id IN (SELECT id FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2) \
         AND NOT EXISTS (SELECT 1 FROM agent_queue_admission d WHERE d.tenant_id = $1 \
         AND d.id = agent_queue.id AND d.phase = 'completed') \
         AND NOT COALESCE(jsonb_path_exists(payload::jsonb, 'lax $.**.discussionMessageId'), false)",
    ),
    ("interrupted_agent", "workspace_id = $2"),
    ("agent_flipped_completion", "workspace_id = $2"),
    ("delegation_group", "workspace_id = $2"),
    (
        "completion_watch",
        "parent_workspace_id = $2 OR child_workspace_id = $2",
    ),
    ("event_subscription", "workspace_id = $2"),
    ("hook", "workspace_id = $2"),
    ("pr_monitor", "workspace_id = $2"),
    ("script", "workspace_id = $2"),
    ("task_agent_link", "workspace_id = $2"),
    ("tracked_changes", "workspace_id = $2"),
    ("diffs", "workspace_id = $2"),
    ("workspace_metrics", "workspace_id = $2"),
    ("agent_metrics", "workspace_id = $2"),
    ("workspace_context_item", "workspace_id = $2"),
    ("workspace_ui_context", "workspace_id = $2"),
    ("attachments", "workspace_id = $2"),
    ("pull_request_workspace", "workspace_id = $2"),
];

/// The scoping predicate of each transfer table, in [`TRANSFER_TABLES`] order.
fn transfer_predicates() -> Result<impl Iterator<Item = (&'static str, &'static str)>> {
    let same = TRANSFER_TABLES.len() == TRANSFER_PREDICATES.len()
        && TRANSFER_TABLES
            .iter()
            .zip(TRANSFER_PREDICATES)
            .all(|((a, _), (b, _))| a == b);
    if !same {
        return Err(Error::Internal(
            "Postgres transfer predicates do not match TRANSFER_TABLES".into(),
        ));
    }
    Ok(TRANSFER_PREDICATES.iter().copied())
}

/// A transfer table's columns as `SQLite` has them: `(name, type)` in
/// declaration order, without the Postgres-only `tenant_id`, `row_seq` and
/// generated search columns.
type TransferColumns = HashMap<String, Vec<(String, String)>>;

fn column_list(columns: &[(String, String)]) -> String {
    columns
        .iter()
        .map(|(c, _)| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One exported Postgres row as a JSON object (the `SQLite` export's shape).
fn row_to_json(table: &str, row: &PgRow) -> Result<serde_json::Value> {
    let fail = |e: sqlx::Error| Error::Internal(format!("transfer export {table}: {e}"));
    let mut object = serde_json::Map::with_capacity(row.columns().len());
    for (i, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(i).map_err(fail)?;
        let value = if raw.is_null() {
            serde_json::Value::Null
        } else {
            match raw.type_info().name() {
                "INT8" => serde_json::json!(row.try_get::<i64, _>(i).map_err(fail)?),
                "INT4" => serde_json::json!(row.try_get::<i32, _>(i).map_err(fail)?),
                "INT2" => serde_json::json!(row.try_get::<i16, _>(i).map_err(fail)?),
                "BOOL" => serde_json::json!(i64::from(row.try_get::<bool, _>(i).map_err(fail)?)),
                "FLOAT8" => serde_json::json!(row.try_get::<f64, _>(i).map_err(fail)?),
                "FLOAT4" => serde_json::json!(row.try_get::<f32, _>(i).map_err(fail)?),
                "BYTEA" => {
                    let bytes = row.try_get::<Vec<u8>, _>(i).map_err(fail)?;
                    serde_json::json!({ "$base64": base64_encode(&bytes) })
                }
                _ => serde_json::json!(row.try_get::<String, _>(i).map_err(fail)?),
            }
        };
        object.insert(column.name().to_string(), value);
    }
    Ok(serde_json::Value::Object(object))
}

/// An imported JSON value ready to bind: text (cast to the column's type in
/// the statement, as `SQLite`'s column affinity converts it) or bytes.
enum ImportValue {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

/// `transfer_repo::bind_json_value`, for Postgres.
fn import_value(table: &str, key: &str, value: &serde_json::Value) -> Result<ImportValue> {
    Ok(match value {
        serde_json::Value::Null => ImportValue::Null,
        serde_json::Value::Bool(b) => ImportValue::Text(i64::from(*b).to_string()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ImportValue::Text(i.to_string())
            } else if let Some(f) = n.as_f64() {
                ImportValue::Text(f.to_string())
            } else {
                return Err(Error::InvalidParams(format!(
                    "transfer import: {table}.{key} has an unrepresentable number"
                )));
            }
        }
        serde_json::Value::String(s) => ImportValue::Text(s.clone()),
        serde_json::Value::Object(o) => {
            let Some(serde_json::Value::String(encoded)) = o.get("$base64") else {
                return Err(Error::InvalidParams(format!(
                    "transfer import: {table}.{key} object value is not a $base64 blob"
                )));
            };
            ImportValue::Bytes(base64_decode(encoded).ok_or_else(|| {
                Error::InvalidParams(format!("transfer import: {table}.{key} invalid base64"))
            })?)
        }
        serde_json::Value::Array(_) => {
            return Err(Error::InvalidParams(format!(
                "transfer import: {table}.{key} array values are not supported"
            )));
        }
    })
}

impl PgStore {
    /// Every transfer table's [`TransferColumns`], in one statement.
    async fn transfer_columns(&self, conn: &mut PgConnection) -> Result<TransferColumns> {
        let tables: Vec<&str> = TRANSFER_TABLES.iter().map(|(t, _)| *t).collect();
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod) \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() AND c.relname = ANY($1) \
             AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '' \
             AND a.attname NOT IN ('tenant_id', 'row_seq') \
             ORDER BY c.relname, a.attnum",
        )
        .bind(&tables)
        .fetch_all(conn)
        .await
        .map_err(|e| Error::Internal(format!("transfer table columns failed: {e}")))?;
        let mut columns = TransferColumns::new();
        for (table, column, ty) in rows {
            columns.entry(table).or_default().push((column, ty));
        }
        Ok(columns)
    }

    pub async fn transfer_table_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TransferTableStat>> {
        let mut tx = self.begin().await?;
        let columns = self.transfer_columns(&mut tx).await?;
        let sql = transfer_predicates()?
            .enumerate()
            .map(|(i, (table, predicate))| {
                let size_expr = match columns.get(table) {
                    Some(cols) if !cols.is_empty() => cols
                        .iter()
                        // SQLite's OCTET_LENGTH counts a number's text form.
                        .map(|(c, ty)| {
                            if ty == "bytea" {
                                format!("COALESCE(OCTET_LENGTH(\"{c}\"), 0)")
                            } else {
                                format!("COALESCE(OCTET_LENGTH(\"{c}\"::text), 0)")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" + "),
                    _ => "0".to_string(),
                };
                format!(
                    "SELECT {i}::bigint AS i, COUNT(*) AS n, \
                     COALESCE(SUM({size_expr}), 0)::bigint AS b \
                     FROM \"{table}\" WHERE tenant_id = $1 AND ({predicate})"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let rows = sqlx::query_as::<_, (i64, i64, i64)>(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("transfer stats aggregate failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        let mut stats: Vec<TransferTableStat> = TRANSFER_TABLES
            .iter()
            .map(|(table, _)| TransferTableStat {
                name: (*table).to_string(),
                row_count: 0,
                approx_bytes: 0,
            })
            .collect();
        for (i, n, b) in rows {
            let Some(stat) = usize::try_from(i).ok().and_then(|i| stats.get_mut(i)) else {
                return Err(Error::Internal(format!(
                    "transfer stats aggregate returned unknown arm index {i}"
                )));
            };
            stat.row_count = n;
            stat.approx_bytes = b;
        }
        Ok(stats)
    }

    /// The rows of `tables` (all transfer tables when `None`) in one
    /// snapshot, plus the export's author snapshots.
    async fn transfer_rows(
        &self,
        workspace_id: &WorkspaceId,
        only: Option<&[&str]>,
        fail: impl Fn(&str, sqlx::Error) -> Error,
    ) -> Result<(
        Transaction<'static, Postgres>,
        Vec<(String, Vec<serde_json::Value>)>,
    )> {
        let mut tx = self.begin_repository_snapshot().await?;
        let columns = self.transfer_columns(&mut tx).await?;
        let mut out = Vec::new();
        for (table, predicate) in transfer_predicates()? {
            if only.is_some_and(|only| !only.contains(&table)) {
                continue;
            }
            let cols = columns.get(table).map(Vec::as_slice).unwrap_or_default();
            let sql = format!(
                "SELECT {} FROM \"{table}\" WHERE tenant_id = $1 AND ({predicate})",
                column_list(cols)
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(self.tenant())
                .bind(&workspace_id.0)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| fail(table, e))?;
            let objects = rows
                .iter()
                .map(|row| row_to_json(table, row))
                .collect::<Result<Vec<_>>>()?;
            out.push((table.to_string(), objects));
        }
        Ok((tx, out))
    }

    pub async fn transfer_export_rows(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<(String, Vec<serde_json::Value>)>> {
        let (mut tx, mut out) = self
            .transfer_rows(workspace_id, None, |table, e| {
                Error::Internal(format!("transfer export {table} failed: {e}"))
            })
            .await?;
        let ids = crate::transfer_authorship::authors_to_snapshot(&out)?;
        let mut people: HashMap<PrincipalId, Principal> = HashMap::new();
        if !ids.is_empty() {
            let ids: Vec<&str> = ids.iter().map(PrincipalId::as_str).collect();
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT {} FROM principal WHERE tenant_id = $1 AND id = ANY($2)",
                crate::principal_repo::PRINCIPAL_COLUMNS
            )))
            .bind(self.tenant())
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("export author snapshot failed: {e}")))?;
            for row in &rows {
                let person = super::principals::principal(row)?;
                people.insert(person.id.clone(), person);
            }
        }
        crate::transfer_authorship::apply_snapshots(&mut out, &people)?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("transfer export commit failed: {e}")))?;
        Ok(out)
    }

    pub async fn transfer_monitor_rows(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<(String, Vec<serde_json::Value>)>> {
        let (tx, out) = self
            .transfer_rows(
                workspace_id,
                Some(&["pr_monitor", "agent_message"]),
                |_, e| Error::Internal(format!("transfer preflight failed: {e}")),
            )
            .await?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("transfer preflight failed: {e}")))?;
        Ok(out)
    }

    pub async fn transfer_import_rows(
        &self,
        rows: &[(String, Vec<serde_json::Value>)],
    ) -> Result<usize> {
        let mut lifecycle = self.repository_lifecycle_write().await?;
        for (table, _) in rows {
            if !TRANSFER_TABLES.iter().any(|(t, _)| t == table) {
                return Err(Error::InvalidParams(format!(
                    "transfer import: table {table} is not part of the transfer set"
                )));
            }
        }
        let imported_agent_ids: Vec<&str> = rows
            .iter()
            .find(|(table, _)| table == "agent_session")
            .into_iter()
            .flat_map(|(_, objects)| objects)
            .filter_map(|object| object.get("id").and_then(serde_json::Value::as_str))
            .collect();

        let mut tx = self.begin_write().await?;
        let schemas = self.transfer_columns(&mut tx).await?;
        let existing_workspaces: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM workspace WHERE tenant_id = $1 \
             UNION SELECT id FROM deleted_workspace_id WHERE tenant_id = $1",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("read delivery import fence: {e}")))?;
        self.reclaim_imported_metadata(&mut tx, rows).await?;
        let mut inserted = 0usize;
        for (table, _) in TRANSFER_TABLES {
            let Some((_, objects)) = rows.iter().find(|(t, _)| t == table) else {
                continue;
            };
            let schema = schemas.get(*table).map(Vec::as_slice).unwrap_or_default();
            for object in objects {
                if matches!(
                    *table,
                    "script_monitor" | "script_notification_job" | "agent_queue_admission"
                ) && object["workspace_id"]
                    .as_str()
                    .is_some_and(|ws| existing_workspaces.iter().any(|id| id == ws))
                {
                    continue;
                }
                let history_metadata = (*table == "agent_message")
                    .then(|| object.get("metadata").and_then(serde_json::Value::as_str))
                    .flatten()
                    .filter(|raw| {
                        serde_json::from_str::<serde_json::Value>(raw)
                            .is_ok_and(|md| md["type"] == "script_monitor_wake")
                    });
                let map = object.as_object().ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "transfer import: {table} row is not a JSON object"
                    ))
                })?;
                let monitor_map = if *table == "pr_monitor" {
                    let mut claim = map.clone();
                    if claim
                        .get("pr_number")
                        .and_then(serde_json::Value::as_i64)
                        .is_none()
                    {
                        return Err(Error::InvalidParams(
                            "transfer import: pr_monitor.pr_number must be an exact signed integer"
                                .into(),
                        ));
                    }
                    let has_target = [
                        "target_provider",
                        "target_instance_base_url",
                        "target_project_path",
                        "target_kind",
                    ]
                    .iter()
                    .any(|key| claim.get(*key).is_some_and(|v| !v.is_null()));
                    claim.insert("target_provenance".into(), "unresolved".into());
                    let reason = if has_target {
                        "unverified-import"
                    } else {
                        "missing-provenance"
                    };
                    claim.insert("target_unresolved_reason".into(), reason.into());
                    Some(claim)
                } else {
                    None
                };
                let map = monitor_map.as_ref().unwrap_or(map);
                let mut columns = Vec::with_capacity(map.len());
                for key in map.keys() {
                    let Some((_, ty)) = schema.iter().find(|(c, _)| c == key) else {
                        return Err(Error::InvalidParams(format!(
                            "transfer import: {table} has no column {key}"
                        )));
                    };
                    columns.push((key.as_str(), ty.as_str()));
                }
                if columns.is_empty() {
                    continue;
                }
                let empty_activity = serde_json::json!("[]");
                let mut values = Vec::with_capacity(map.len());
                for (key, value) in map {
                    values.push(import_value(
                        table,
                        key,
                        if *table == "workspace_discussion_message"
                            && matches!(
                                key.as_str(),
                                "agent_activity_json" | "principal_mentions_json"
                            )
                        {
                            &empty_activity
                        } else if history_metadata.is_some() && key == "metadata" {
                            &serde_json::Value::Null
                        } else {
                            value
                        },
                    )?);
                }
                let names = columns
                    .iter()
                    .map(|(c, _)| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = columns
                    .iter()
                    .enumerate()
                    .map(|(i, (_, ty))| format!("${}::{ty}", i + 2))
                    .collect::<Vec<_>>()
                    .join(", ");
                // SQLite's INSERT OR REPLACE: the archive's link wins.
                let conflict = if *table == "pull_request_workspace" {
                    let set = columns
                        .iter()
                        .map(|(c, _)| format!("\"{c}\" = excluded.\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(" ON CONFLICT (tenant_id, pr_key, workspace_id) DO UPDATE SET {set}")
                } else {
                    String::new()
                };
                let sql = format!(
                    "INSERT INTO \"{table}\" (tenant_id, {names}) VALUES ($1, {placeholders}){conflict}"
                );
                if inserted == 0 {
                    lifecycle.begin(&[RepositoryLifecycleKey::Database])?;
                }
                let selection_before = if *table == "workspace" {
                    let id = map
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            Error::InvalidParams("transfer workspace id must be text".into())
                        })?;
                    let id = WorkspaceId::from(id);
                    let before = self.imported_primary_before(&mut tx, &id).await?;
                    Some((id, before))
                } else {
                    None
                };
                if *table == "agent_session" {
                    sqlx::query(
                        "DELETE FROM review_context_enrollment WHERE tenant_id = $1 AND agent_id = $2",
                    )
                    .bind(self.tenant())
                    .bind(object["id"].as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| {
                        Error::Internal("import native enrollment invalidation failed".into())
                    })?;
                }
                if *table == "agent_message" {
                    sqlx::query(
                        "DELETE FROM pr_monitor_message_provenance \
                         WHERE tenant_id = $1 AND message_id = $2",
                    )
                    .bind(self.tenant())
                    .bind(object["id"].as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| {
                        Error::Internal("import monitor provenance invalidation failed".into())
                    })?;
                }
                let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
                for value in values {
                    query = match value {
                        ImportValue::Null => query.bind(Option::<String>::None),
                        ImportValue::Text(s) => query.bind(s),
                        ImportValue::Bytes(b) => query.bind(b),
                    };
                }
                query.execute(&mut *tx).await.map_err(|e| {
                    Error::Internal(format!("transfer import insert into {table} failed: {e}"))
                })?;
                if let Some(metadata) = history_metadata {
                    sqlx::query(
                        "UPDATE agent_message SET metadata = $4 \
                         WHERE tenant_id = $1 AND id = $2 AND agent_id = $3",
                    )
                    .bind(self.tenant())
                    .bind(object["id"].as_str())
                    .bind(object["agent_id"].as_str())
                    .bind(metadata)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("restore imported monitor history: {e}"))
                    })?;
                }
                if let Some((id, before)) = selection_before {
                    self.classify_imported_primary(&mut tx, &id, before).await?;
                }
                inserted += 1;
            }
        }
        if !imported_agent_ids.is_empty() {
            self.materialize_imported_usage(&mut tx, &imported_agent_ids)
                .await?;
        }
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("transfer import commit failed: {e}")))?;
        lifecycle.settle_after_retirement().await?;
        Ok(inserted)
    }

    /// The import's usage provenance and per-model usage cells, from the
    /// imported sessions' messages and token snapshots.
    async fn materialize_imported_usage(&self, tx: &mut WriteTx, agent_ids: &[&str]) -> Result<()> {
        sqlx::query(
            "UPDATE agent_message SET \
             usage_model = COALESCE(NULLIF(usage_model, ''), \
               COALESCE(NULLIF((SELECT s.resolved_model FROM agent_session s \
                                WHERE s.tenant_id = $1 AND s.id = agent_message.agent_id), ''), \
                        NULLIF((SELECT s.model FROM agent_session s \
                                WHERE s.tenant_id = $1 AND s.id = agent_message.agent_id), ''), \
                        'unknown')), \
             usage_origin = COALESCE(usage_origin, CASE \
               WHEN role = 'assistant' THEN 'agent' \
               WHEN role = 'user' AND CASE WHEN sqlite_json_valid(metadata) \
                 THEN COALESCE(NULLIF(sqlite_json_extract(metadata, '$.fromAgentId'), ''), '') \
                 ELSE '' END <> '' THEN 'agent' \
               WHEN role = 'user' THEN 'human' ELSE 'excluded' END) \
             WHERE tenant_id = $1 AND agent_id = ANY($2) \
             AND (NULLIF(usage_model, '') IS NULL OR usage_origin IS NULL)",
        )
        .bind(self.tenant())
        .bind(agent_ids)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("materialize imported provenance failed: {e}")))?;

        let session_rows: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT s.id, COALESCE(NULLIF(s.resolved_model, ''), NULLIF(s.model, ''), 'unknown'), \
             s.token_usage, s.token_usage_baseline FROM agent_session s \
             WHERE s.tenant_id = $1 AND s.id = ANY($2) \
             AND NOT EXISTS (SELECT 1 FROM agent_usage_cell c \
                             WHERE c.tenant_id = $1 AND c.agent_id = s.id) \
             ORDER BY s.row_seq",
        )
        .bind(self.tenant())
        .bind(agent_ids)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("read imported usage snapshots failed: {e}")))?;
        for (id, model, token_usage, token_usage_baseline) in session_rows {
            let snapshot =
                token_usage.and_then(|raw| serde_json::from_str::<TokenUsageTotals>(&raw).ok());
            let baseline = token_usage_baseline
                .and_then(|raw| serde_json::from_str::<TokenUsageTotals>(&raw).ok());
            if snapshot.is_none() && baseline.is_none() {
                continue;
            }
            let snapshot = snapshot.unwrap_or_default();
            let baseline = baseline.unwrap_or_default();
            let mut costs = BTreeMap::<String, f64>::new();
            for cost in [baseline.cost.as_ref(), snapshot.cost.as_ref()]
                .into_iter()
                .flatten()
            {
                *costs.entry(cost.currency.clone()).or_default() += cost.amount;
            }
            let costs_json = serde_json::to_string(&costs)
                .map_err(|e| Error::Internal(format!("encode imported usage costs failed: {e}")))?;
            let to_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
            sqlx::query(
                "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, input_tokens, \
                 output_tokens, cache_read_tokens, cache_creation_tokens, thought_tokens, \
                 costs_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (tenant_id, agent_id, model) DO NOTHING",
            )
            .bind(self.tenant())
            .bind(id)
            .bind(model)
            .bind(to_i64(
                baseline.input_tokens.saturating_add(snapshot.input_tokens),
            ))
            .bind(to_i64(
                baseline
                    .output_tokens
                    .saturating_add(snapshot.output_tokens),
            ))
            .bind(to_i64(
                baseline
                    .cache_read_tokens
                    .saturating_add(snapshot.cache_read_tokens),
            ))
            .bind(to_i64(
                baseline
                    .cache_creation_tokens
                    .saturating_add(snapshot.cache_creation_tokens),
            ))
            .bind(to_i64(
                baseline
                    .thought_tokens
                    .saturating_add(snapshot.thought_tokens),
            ))
            .bind(costs_json)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("materialize imported usage failed: {e}")))?;
        }
        sqlx::query(
            "INSERT INTO agent_usage_cell \
             (tenant_id, agent_id, model, human_messages, agent_messages) \
             SELECT $1, agent_id, COALESCE(NULLIF(usage_model, ''), 'unknown'), \
               SUM(CASE WHEN usage_origin = 'human' THEN 1 ELSE 0 END), \
               SUM(CASE WHEN role = 'assistant' OR usage_origin = 'agent' THEN 1 ELSE 0 END) \
             FROM agent_message WHERE tenant_id = $1 AND agent_id = ANY($2) \
             AND role IN ('user', 'assistant') \
             GROUP BY agent_id, COALESCE(NULLIF(usage_model, ''), 'unknown') \
             ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
               human_messages = excluded.human_messages, \
               agent_messages = excluded.agent_messages",
        )
        .bind(self.tenant())
        .bind(agent_ids)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Internal(format!("materialize imported counts failed: {e}")))?;
        Ok(())
    }

    /// `transfer_repo::reclaim_imported_metadata`.
    async fn reclaim_imported_metadata(
        &self,
        tx: &mut WriteTx,
        rows: &[(String, Vec<serde_json::Value>)],
    ) -> Result<()> {
        let workspaces: HashSet<&str> = rows
            .iter()
            .find(|(table, _)| table == "workspace")
            .into_iter()
            .flat_map(|(_, objects)| objects)
            .filter_map(|row| row.get("id")?.as_str())
            .collect();
        for (table, key) in [
            ("interrupted_agent", "agent_id"),
            ("script", "id"),
            ("attachments", "id"),
        ] {
            let Some((_, objects)) = rows.iter().find(|(t, _)| t == table) else {
                continue;
            };
            for row in objects {
                let (Some(id), Some(workspace)) = (
                    row.get(key).and_then(serde_json::Value::as_str),
                    row.get("workspace_id").and_then(serde_json::Value::as_str),
                ) else {
                    continue;
                };
                if !workspaces.contains(workspace) {
                    continue;
                }
                if table == "attachments" {
                    loop {
                        let removed = sqlx::query(
                            "DELETE FROM attachment_idempotency_keys \
                             WHERE tenant_id = $1 AND (workspace_id, \"key\") IN \
                             (SELECT k.workspace_id, k.\"key\" FROM attachment_idempotency_keys k \
                              JOIN attachments a ON a.tenant_id = $1 AND a.id = k.attachment_id \
                              WHERE k.tenant_id = $1 AND a.id = $2 AND a.workspace_id = $3 \
                                AND k.workspace_id = $3 \
                                AND NOT EXISTS (SELECT 1 FROM workspace \
                                                WHERE tenant_id = $1 AND id = $3) \
                              LIMIT $4)",
                        )
                        .bind(self.tenant())
                        .bind(id)
                        .bind(workspace)
                        .bind(crate::agent_repo::DELETE_CASCADE_BATCH)
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("reclaim attachment retry keys failed: {e}"))
                        })?
                        .rows_affected();
                        if removed < crate::agent_repo::DELETE_CASCADE_BATCH.unsigned_abs() {
                            break;
                        }
                    }
                }
                let agent_guard = if table == "interrupted_agent" {
                    " AND NOT EXISTS (SELECT 1 FROM agent_session WHERE tenant_id = $1 AND id = $2)"
                } else {
                    ""
                };
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE tenant_id = $1 AND {key} = $2 AND workspace_id = $3 \
                     AND NOT EXISTS (SELECT 1 FROM workspace WHERE tenant_id = $1 AND id = $3)\
                     {agent_guard}"
                )))
                .bind(self.tenant())
                .bind(id)
                .bind(workspace)
                .execute(&mut **tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("reclaim imported {table} metadata failed: {e}"))
                })?;
            }
        }
        Ok(())
    }
}
