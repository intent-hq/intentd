//! Atomic guards for volatile note deletion. This stores no trash or receipts.
use crate::{note_write_connection::NoteWriteConnection, Store};
use intent_core::note_delete::{
    valid_identifier, NoteDeleteError, NoteDeleteIdentity, NoteDeleteReason, NoteDeleteSchedule,
    MAX_CHILDREN, MAX_SAFE_INTEGER,
};
use intent_core::{Caller, Error, NoteId, Result, WorkspaceId};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

/// Original request authority, never a timer's daemon privilege.
#[derive(Clone)]
pub struct NoteDeleteAuthority {
    pub caller: Caller,
    pub principal_token_hash: Option<String>,
}
/// Small immutable preparation guard; no note or child bodies are retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoteDeleteGuard {
    pub identity: NoteDeleteIdentity,
    pub children_count: usize,
    pub children_digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoteDeleteCommitOutcome {
    Deleted,
    Rejected(NoteDeleteReason),
    Failed,
    OutcomeUnknown,
}
fn failure(code: NoteDeleteError) -> Error {
    Error::NoteDelete(code)
}
fn db(error: &sqlx::Error) -> Error {
    Error::Internal(format!("note delete storage: {error}"))
}

async fn authorize(
    conn: &mut SqliteConnection,
    authority: &NoteDeleteAuthority,
    ws: &WorkspaceId,
) -> Result<()> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace WHERE id=?)")
        .bind(ws.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(|error| db(&error))?;
    if !exists {
        return Err(failure(NoteDeleteError::Unavailable));
    }
    match &authority.caller {
        Caller::Daemon => Ok(()),
        Caller::Agent { agent_id } => {
            let live: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_session WHERE id=? AND retired_at IS NULL)",
            )
            .bind(agent_id.as_str())
            .fetch_one(&mut *conn)
            .await
            .map_err(|error| db(&error))?;
            if live {
                Ok(())
            } else {
                Err(failure(NoteDeleteError::Forbidden))
            }
        }
        Caller::Wire { principal_id, .. } => {
            let primary: Option<bool> =
                sqlx::query_scalar("SELECT is_primary FROM principal WHERE id=?")
                    .bind(principal_id.as_str())
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(|error| db(&error))?;
            let Some(primary) = primary else {
                return Err(failure(NoteDeleteError::Forbidden));
            };
            if let Some(hash) = &authority.principal_token_hash {
                let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM principal_credential WHERE token_hash=? AND principal_id=? AND revoked_at IS NULL)")
                    .bind(hash).bind(principal_id.as_str()).fetch_one(&mut *conn).await.map_err(|error| db(&error))?;
                if !valid {
                    return Err(failure(NoteDeleteError::Forbidden));
                }
            }
            if primary {
                return Ok(());
            }
            let member: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM host_member WHERE principal_id=?)")
                    .bind(principal_id.as_str())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(|error| db(&error))?;
            if member && !ws.is_chief() {
                return Ok(());
            }
            let guest: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace_member WHERE workspace_id=? AND principal_id=? AND role IN ('owner','collaborator'))")
                .bind(ws.as_str()).bind(principal_id.as_str()).fetch_one(&mut *conn).await.map_err(|error| db(&error))?;
            if guest && !member {
                Ok(())
            } else {
                Err(failure(NoteDeleteError::Unavailable))
            }
        }
    }
}
async fn identity(
    conn: &mut SqliteConnection,
    ws: &WorkspaceId,
    note: &NoteId,
) -> Result<Option<NoteDeleteIdentity>> {
    let row = sqlx::query("SELECT n.rev,h.instance_id,h.generation,h.current_rev,h.indexed_rev FROM note n LEFT JOIN note_page_head h ON h.workspace_id=n.workspace_id AND h.note_id=n.id WHERE n.workspace_id=? AND n.id=?")
        .bind(ws.as_str()).bind(note.as_str()).fetch_optional(&mut *conn).await.map_err(|error| db(&error))?;
    row.map(|row| {
        let revision: i64 = row.try_get("rev").map_err(|error| db(&error))?;
        let instance: Option<String> = row.try_get("instance_id").map_err(|error| db(&error))?;
        let generation: Option<String> = row.try_get("generation").map_err(|error| db(&error))?;
        let current: Option<i64> = row.try_get("current_rev").map_err(|error| db(&error))?;
        let indexed: Option<i64> = row.try_get("indexed_rev").map_err(|error| db(&error))?;
        let (Some(instance), Some(generation)) = (instance, generation) else {
            return Err(failure(NoteDeleteError::Unavailable));
        };
        if current != Some(revision) || indexed != current || !valid_identifier(&generation) {
            return Err(failure(NoteDeleteError::Unavailable));
        }
        let source_revision = format!("r:{revision}:{generation}");
        if !u64::try_from(revision).is_ok_and(|value| value <= MAX_SAFE_INTEGER)
            || !valid_identifier(&instance)
            || !valid_identifier(&source_revision)
        {
            return Err(failure(NoteDeleteError::Unavailable));
        }
        Ok(NoteDeleteIdentity {
            note_instance_id: instance,
            revision,
            source_revision,
        })
    })
    .transpose()
}
async fn children(
    conn: &mut SqliteConnection,
    ws: &WorkspaceId,
    note: &NoteId,
) -> Result<(usize, [u8; 32])> {
    let rows = sqlx::query("SELECT n.id,n.rev,h.instance_id,h.generation,h.current_rev,h.indexed_rev FROM note n LEFT JOIN note_page_head h ON h.workspace_id=n.workspace_id AND h.note_id=n.id WHERE n.workspace_id=? AND n.parent_id=? ORDER BY n.id LIMIT 257")
        .bind(ws.as_str()).bind(note.as_str()).fetch_all(&mut *conn).await.map_err(|error| db(&error))?;
    if rows.len() > MAX_CHILDREN {
        return Err(failure(NoteDeleteError::GraphLimit));
    }
    let mut digest = Sha256::new();
    for row in &rows {
        for field in ["id", "instance_id", "generation"] {
            let value: Option<String> = row.try_get(field).map_err(|error| db(&error))?;
            let Some(value) = value.filter(|v| valid_identifier(v)) else {
                return Err(failure(NoteDeleteError::GraphLimit));
            };
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        let revision: i64 = row.try_get("rev").map_err(|error| db(&error))?;
        let current: Option<i64> = row.try_get("current_rev").map_err(|error| db(&error))?;
        let indexed: Option<i64> = row.try_get("indexed_rev").map_err(|error| db(&error))?;
        if !u64::try_from(revision).is_ok_and(|value| value <= MAX_SAFE_INTEGER)
            || current != Some(revision)
            || indexed != current
        {
            return Err(failure(NoteDeleteError::GraphLimit));
        }
        digest.update(revision.to_be_bytes());
    }
    Ok((rows.len(), digest.finalize().into()))
}
fn expected(request: &NoteDeleteSchedule) -> NoteDeleteIdentity {
    NoteDeleteIdentity {
        note_instance_id: request.note_instance_id.clone(),
        revision: request.expected_version,
        source_revision: request.source_revision.clone(),
    }
}
impl Store {
    /// Small current identity and current authorization in one read snapshot.
    ///
    /// # Errors
    /// Returns an error if authorization fails, the note head is unavailable,
    /// or the read transaction fails.
    pub async fn note_delete_current(
        &self,
        authority: &NoteDeleteAuthority,
        ws: &WorkspaceId,
        note: Option<&NoteId>,
    ) -> Result<Option<NoteDeleteIdentity>> {
        let mut tx = self.read_pool().begin().await.map_err(|error| db(&error))?;
        authorize(&mut tx, authority, ws).await?;
        let current = if let Some(note) = note {
            identity(&mut tx, ws, note).await?
        } else {
            None
        };
        tx.commit().await.map_err(|error| db(&error))?;
        Ok(current)
    }
    /// Capture bounded children only after a service reservation owns capacity.
    ///
    /// # Errors
    /// Returns an error for failed authorization, stale or unavailable identity,
    /// an invalid or oversized child graph, or a failed read transaction.
    pub async fn note_delete_prepare(
        &self,
        authority: &NoteDeleteAuthority,
        request: &NoteDeleteSchedule,
    ) -> Result<NoteDeleteGuard> {
        let mut tx = self.read_pool().begin().await.map_err(|error| db(&error))?;
        authorize(&mut tx, authority, &request.workspace_id).await?;
        let current = identity(&mut tx, &request.workspace_id, &request.note_id)
            .await?
            .ok_or_else(|| failure(NoteDeleteError::Unavailable))?;
        if current != expected(request) {
            return Err(failure(NoteDeleteError::Stale));
        }
        let (children_count, children_digest) =
            children(&mut tx, &request.workspace_id, &request.note_id).await?;
        tx.commit().await.map_err(|error| db(&error))?;
        Ok(NoteDeleteGuard {
            identity: current,
            children_count,
            children_digest,
        })
    }
    /// Own the writer through commit/rollback. An indeterminate COMMIT is never
    /// described as a safe rollback; its service receipt must stay uncertain.
    ///
    /// # Errors
    /// Returns an error if acquiring or opening the owned writer fails. Once
    /// opened, transaction failures are represented by the returned outcome.
    pub async fn note_delete_guarded_commit(
        &self,
        authority: &NoteDeleteAuthority,
        request: &NoteDeleteSchedule,
        guard: &NoteDeleteGuard,
        acquire_deadline: tokio::time::Instant,
    ) -> Result<NoteDeleteCommitOutcome> {
        let mut conn = NoteWriteConnection::begin_before(self, acquire_deadline).await?;
        let result = async {
            if let Err(error) = authorize(&mut conn, authority, &request.workspace_id).await {
                return match error {
                    Error::NoteDelete(NoteDeleteError::Unavailable) => Ok(
                        NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::WorkspaceMissing),
                    ),
                    Error::NoteDelete(NoteDeleteError::Forbidden) => Ok(
                        NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::AuthorityLost),
                    ),
                    other => Err(other),
                };
            }
            let Some(current) =
                identity(&mut conn, &request.workspace_id, &request.note_id).await?
            else {
                return Ok(NoteDeleteCommitOutcome::Rejected(
                    NoteDeleteReason::NoteMissing,
                ));
            };
            if current != guard.identity || current != expected(request) {
                return Ok(NoteDeleteCommitOutcome::Rejected(
                    NoteDeleteReason::NoteChanged,
                ));
            }
            let observed = children(&mut conn, &request.workspace_id, &request.note_id).await;
            match observed {
                Ok((count, digest))
                    if count == guard.children_count && digest == guard.children_digest => {}
                Ok(_) | Err(Error::NoteDelete(NoteDeleteError::GraphLimit)) => {
                    return Ok(NoteDeleteCommitOutcome::Rejected(
                        NoteDeleteReason::ChildChanged,
                    ))
                }
                Err(error) => return Err(error),
            }
            // The validated direct children are the only surviving rows changed
            // by the parent-null trigger. Keep their bounded IDs only within
            // this transaction, then finalize their persisted-source anchors.
            let child_ids: Vec<String> = sqlx::query_scalar(
                "SELECT id FROM note WHERE workspace_id=? AND parent_id=? ORDER BY id LIMIT 257",
            )
            .bind(request.workspace_id.as_str())
            .bind(request.note_id.as_str())
            .fetch_all(&mut *conn)
            .await
            .map_err(|error| db(&error))?;
            if child_ids.len() != guard.children_count || child_ids.len() > MAX_CHILDREN {
                return Ok(NoteDeleteCommitOutcome::Rejected(
                    NoteDeleteReason::ChildChanged,
                ));
            }
            let deleted = sqlx::query("DELETE FROM note WHERE workspace_id=? AND id=? AND rev=?")
                .bind(request.workspace_id.as_str())
                .bind(request.note_id.as_str())
                .bind(request.expected_version)
                .execute(&mut *conn)
                .await
                .map_err(|error| db(&error))?;
            if deleted.rows_affected() != 1 {
                return Ok(NoteDeleteCommitOutcome::Rejected(
                    NoteDeleteReason::NoteChanged,
                ));
            }
            crate::note_page_index::rebuild_pending(&mut conn).await?;
            for child in child_ids {
                crate::note_annotation_repo::rebuild_note_anchors(
                    &mut conn,
                    &request.workspace_id,
                    &NoteId::from(child),
                    None,
                )
                .await?;
            }
            Ok(NoteDeleteCommitOutcome::Deleted)
        }
        .await;
        let algorithm_failed = result.is_err();
        match conn.finish(result, "commit guarded note delete").await {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                tracing::warn!(%error, algorithm_failed, "guarded note deletion did not report a successful commit");
                Ok(if algorithm_failed {
                    NoteDeleteCommitOutcome::Failed
                } else {
                    NoteDeleteCommitOutcome::OutcomeUnknown
                })
            }
        }
    }
}
