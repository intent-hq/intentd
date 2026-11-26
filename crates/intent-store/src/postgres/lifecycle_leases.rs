//! Workspace deletion admission (Postgres); `lifecycle_leases.rs` states
//! the rules.
//!
//! A producer write (agent-session insert, user-message append) reads the
//! workspace's deletion lease inside its own write transaction. Both that
//! transaction and the lease acquire take the tenant writer lock, so they
//! are ordered: a producer write that committed before the deleter acquired
//! the lease is in the deleter's session snapshot, and one that starts after
//! sees the lease and fails. The deleting process's own writes are not
//! refused here (`holder` is this process): its local admission gate
//! already closed them.

use intent_core::{AgentId, Error, Result, WorkspaceId};

use super::{db, PgStore, WriteTx};
use crate::lifecycle_leases::WORKSPACE_DELETION_SCOPE;

/// Whether another process ($4) holds the deletion lease of workspace $3
/// in tenant $1 ($2 = the deletion scope).
const DELETING_SQL: &str = "SELECT EXISTS (SELECT 1 FROM lease \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id IS NOT NULL AND expires_at > clock_timestamp() \
     AND holder IS DISTINCT FROM $4)";

/// [`DELETING_SQL`] for the workspace of agent $3; `NULL` (no such agent)
/// reads as not deleting, so the write fails on its own terms.
const AGENT_DELETING_SQL: &str = "SELECT EXISTS (SELECT 1 FROM lease l \
     JOIN agent_session s ON s.tenant_id = $1 AND s.workspace_id = l.lease_key \
     WHERE l.tenant_id = $1 AND l.scope = $2 AND s.id = $3 \
     AND l.lease_id IS NOT NULL AND l.expires_at > clock_timestamp() \
     AND l.holder IS DISTINCT FROM $4)";

pub(super) fn being_deleted(workspace: &str) -> Error {
    Error::NotFound(format!("workspace {workspace} is being deleted"))
}

impl PgStore {
    /// Inside a producer's write transaction: fails with `Error::NotFound`
    /// while another process deletes `workspace_id`.
    pub(super) async fn refuse_if_workspace_deleting(
        &self,
        tx: &mut WriteTx,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        let deleting = sqlx::query_scalar::<_, bool>(DELETING_SQL)
            .bind(self.tenant())
            .bind(WORKSPACE_DELETION_SCOPE)
            .bind(workspace_id.as_str())
            .bind(self.process_token())
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        if deleting {
            return Err(being_deleted(workspace_id.as_str()));
        }
        Ok(())
    }

    /// [`Self::refuse_if_workspace_deleting`] for the workspace of
    /// `agent_id`.
    pub(super) async fn refuse_if_agent_workspace_deleting(
        &self,
        tx: &mut WriteTx,
        agent_id: &AgentId,
    ) -> Result<()> {
        let deleting = sqlx::query_scalar::<_, bool>(AGENT_DELETING_SQL)
            .bind(self.tenant())
            .bind(WORKSPACE_DELETION_SCOPE)
            .bind(&agent_id.0)
            .bind(self.process_token())
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        if deleting {
            return Err(Error::NotFound(format!(
                "the workspace of agent {} is being deleted",
                agent_id.0
            )));
        }
        Ok(())
    }
}
