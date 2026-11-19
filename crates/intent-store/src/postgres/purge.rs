//! Bounded, resumable workspace and agent deletes.
//!
//! One writer transaction over every child row of a large workspace or agent
//! would hold the tenant writer lock for the whole delete, and could run into
//! the writer `statement_timeout`. Instead a delete:
//!
//! 1. records itself in `delete_purge` in a short writer transaction (with
//!    its existence check and lifecycle bracket);
//! 2. drains the heavy child tables in batches of at most
//!    [`PURGE_BATCH_ROWS`] rows, each batch its own writer transaction, so
//!    other writers of the tenant run between batches;
//! 3. runs the unchanged single-transaction delete, which now only sweeps
//!    what is left, and drops the record in that same transaction.
//!
//! So once a delete reports success, the store is exactly as the one-shot
//! delete left it. Meanwhile, as on `SQLite` (`delete_in_bounded_batches`),
//! the workspace or agent stays visible with part of its children gone.
//!
//! Resuming: a delete stopped part-way (a crash, a failed batch) keeps its
//! record, and its workspace or agent stays half-purged, as on `SQLite`,
//! until it is deleted again: the next `delete_workspace` or
//! `delete_agent_session` for it drains what is left (every step is
//! idempotent) and finishes. The store does not finish it on its own: that
//! would skip the service teardown (events, worktree and script cleanup,
//! home stops) that a delete call runs. [`PgStore::pending_deletes`] lists
//! the records, for a service to re-issue those deletes.

use super::{db, PgStore};
use intent_core::{AgentId, Error, Result, WorkspaceId};

/// Most rows one purge batch deletes (`SQLite`'s `DELETE_CASCADE_BATCH`).
pub(super) const PURGE_BATCH_ROWS: i64 = 500;

/// One agent's heavy children, deleted before its session row, in the
/// one-shot delete's order, then the rows the session row's delete trigger
/// (`script_monitor_owner_deleted`) or cascade would otherwise remove in
/// one statement: script monitors and their notification jobs, queue
/// admissions, and discussion deliveries and activity (one per discussion
/// message the agent was sent). `$1` tenant, `$2` agent, `$3` batch size.
/// Left to the final transaction: at most a few rows per agent
/// (`agent_usage_cell` per model, `interrupted_agent`,
/// `agent_stop_redelivery`, `remote_agent`, `review_context_enrollment`,
/// `discussion_agent_grant` per conversation, `agent_flipped_completion` per
/// task); `pr_monitor_message_provenance` goes with its message, batch by
/// batch.
const AGENT_PURGE: [&str; 12] = [
    "DELETE FROM agent_message_payload WHERE tenant_id = $1 \
     AND (message_id, block_ordinal, kind) IN (SELECT p.message_id, p.block_ordinal, p.kind \
       FROM agent_message_payload p WHERE p.tenant_id = $1 AND p.agent_id = $2 LIMIT $3)",
    "DELETE FROM agent_message WHERE tenant_id = $1 \
     AND id IN (SELECT m.id FROM agent_message m \
       WHERE m.tenant_id = $1 AND m.agent_id = $2 LIMIT $3)",
    "DELETE FROM agent_queue WHERE tenant_id = $1 \
     AND id IN (SELECT q.id FROM agent_queue q \
       WHERE q.tenant_id = $1 AND q.agent_id = $2 LIMIT $3)",
    "DELETE FROM hook WHERE tenant_id = $1 \
     AND hook_id IN (SELECT h.hook_id FROM hook h \
       WHERE h.tenant_id = $1 AND h.agent_id = $2 LIMIT $3)",
    "DELETE FROM pr_monitor WHERE tenant_id = $1 \
     AND monitor_id IN (SELECT m.monitor_id FROM pr_monitor m \
       WHERE m.tenant_id = $1 AND m.agent_id = $2 LIMIT $3)",
    "DELETE FROM completion_wake_delivery WHERE tenant_id = $1 \
     AND (parent_agent_id, child_agent_id) IN (SELECT d.parent_agent_id, d.child_agent_id \
       FROM completion_wake_delivery d WHERE d.tenant_id = $1 \
       AND (d.parent_agent_id = $2 OR d.child_agent_id = $2) LIMIT $3)",
    "DELETE FROM advisory_wake_delivery WHERE tenant_id = $1 \
     AND (parent_agent_id, child_agent_id) IN (SELECT d.parent_agent_id, d.child_agent_id \
       FROM advisory_wake_delivery d WHERE d.tenant_id = $1 \
       AND (d.parent_agent_id = $2 OR d.child_agent_id = $2) LIMIT $3)",
    "DELETE FROM script_monitor WHERE tenant_id = $1 \
     AND id IN (SELECT sm.id FROM script_monitor sm \
       WHERE sm.tenant_id = $1 AND sm.agent_id = $2 LIMIT $3)",
    "DELETE FROM script_notification_job WHERE tenant_id = $1 \
     AND wake_id IN (SELECT j.wake_id FROM script_notification_job j \
       WHERE j.tenant_id = $1 AND j.agent_id = $2 LIMIT $3)",
    "DELETE FROM agent_queue_admission WHERE tenant_id = $1 \
     AND id IN (SELECT qa.id FROM agent_queue_admission qa \
       WHERE qa.tenant_id = $1 AND qa.agent_id = $2 LIMIT $3)",
    "DELETE FROM discussion_agent_delivery WHERE tenant_id = $1 AND agent_id = $2 \
     AND message_id IN (SELECT dd.message_id FROM discussion_agent_delivery dd \
       WHERE dd.tenant_id = $1 AND dd.agent_id = $2 LIMIT $3)",
    "DELETE FROM discussion_agent_activity WHERE tenant_id = $1 AND agent_id = $2 \
     AND message_id IN (SELECT da.message_id FROM discussion_agent_activity da \
       WHERE da.tenant_id = $1 AND da.agent_id = $2 LIMIT $3)",
];

/// A workspace's heavy children, after its agents', in the one-shot
/// delete's order. The final transaction still deletes the rest in one go:
/// its explicit statements (recovery history, desktop records, browser
/// tabs, MCP disables, agent preferences, the session rows) and what
/// cascades from the workspace and session rows without a drain here, all
/// at most a few rows per workspace, agent, conversation or message: the
/// per-agent rows listed at [`AGENT_PURGE`], `browser_tab`,
/// `discussion_conversation`, `workspace_discussion_conversation` and
/// `discussion_participant` (per conversation), `discussion_reaction` (with
/// its message, batch by batch), `home_move`, `home_rebuild`,
/// `workspace_home`, `workspace_invite_seat`, `workspace_member`,
/// `workspace_metrics`, `workspace_sharing_summary` and
/// `workspace_ui_context`. The workspace row's delete trigger
/// (`script_monitor_workspace_deleted`) finds no script monitor, job or
/// admission left: every one names an agent of the workspace, drained above. `$1` tenant, `$2` workspace, `$3` batch size. Rows the
/// one-shot delete keeps stay: comments with no note, and an attachment
/// another workspace's retry key still names (the final transaction then
/// fails on it, as the one-shot delete does).
const WORKSPACE_PURGE: [&str; 21] = [
    "DELETE FROM script WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT s.id FROM script s \
       WHERE s.tenant_id = $1 AND s.workspace_id = $2 LIMIT $3)",
    "DELETE FROM attachment_idempotency_keys WHERE tenant_id = $1 AND workspace_id = $2 \
     AND \"key\" IN (SELECT k.\"key\" FROM attachment_idempotency_keys k \
       WHERE k.tenant_id = $1 AND k.workspace_id = $2 \
       AND EXISTS (SELECT 1 FROM attachments a WHERE a.tenant_id = $1 \
         AND a.id = k.attachment_id AND a.workspace_id = $2) LIMIT $3)",
    "DELETE FROM attachments WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT a.id FROM attachments a WHERE a.tenant_id = $1 AND a.workspace_id = $2 \
       AND NOT EXISTS (SELECT 1 FROM attachment_idempotency_keys k \
         WHERE k.tenant_id = $1 AND k.attachment_id = a.id) LIMIT $3)",
    // Clear parent links first so a note's delete trigger updates no child.
    "UPDATE note SET parent_id = NULL WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT n.id FROM note n WHERE n.tenant_id = $1 AND n.workspace_id = $2 \
       AND n.parent_id IS NOT NULL LIMIT $3)",
    "DELETE FROM note_page_entry WHERE tenant_id = $1 AND workspace_id = $2 \
     AND (note_id, collection, position) IN (SELECT e.note_id, e.collection, e.position \
       FROM note_page_entry e WHERE e.tenant_id = $1 AND e.workspace_id = $2 LIMIT $3)",
    "DELETE FROM note_page_head WHERE tenant_id = $1 AND workspace_id = $2 \
     AND note_id IN (SELECT h.note_id FROM note_page_head h \
       WHERE h.tenant_id = $1 AND h.workspace_id = $2 LIMIT $3)",
    "DELETE FROM comment WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT c.id FROM comment c WHERE c.tenant_id = $1 AND c.workspace_id = $2 \
       AND c.note_id IS NOT NULL LIMIT $3)",
    "DELETE FROM note_version WHERE tenant_id = $1 AND workspace_id = $2 \
     AND (note_id, v) IN (SELECT nv.note_id, nv.v FROM note_version nv \
       WHERE nv.tenant_id = $1 AND nv.workspace_id = $2 LIMIT $3)",
    "DELETE FROM note_line_attribution WHERE tenant_id = $1 AND workspace_id = $2 \
     AND note_id IN (SELECT la.note_id FROM note_line_attribution la \
       WHERE la.tenant_id = $1 AND la.workspace_id = $2 LIMIT $3)",
    "DELETE FROM note WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT n.id FROM note n \
       WHERE n.tenant_id = $1 AND n.workspace_id = $2 LIMIT $3)",
    "DELETE FROM tracked_changes WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT tc.id FROM tracked_changes tc \
       WHERE tc.tenant_id = $1 AND tc.workspace_id = $2 LIMIT $3)",
    "DELETE FROM diffs WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT d.id FROM diffs d \
       WHERE d.tenant_id = $1 AND d.workspace_id = $2 LIMIT $3)",
    "DELETE FROM delegation_group WHERE tenant_id = $1 AND workspace_id = $2 \
     AND group_id IN (SELECT g.group_id FROM delegation_group g \
       WHERE g.tenant_id = $1 AND g.workspace_id = $2 LIMIT $3)",
    "DELETE FROM task_agent_link WHERE tenant_id = $1 AND workspace_id = $2 \
     AND (note_id, task_key) IN (SELECT l.note_id, l.task_key FROM task_agent_link l \
       WHERE l.tenant_id = $1 AND l.workspace_id = $2 LIMIT $3)",
    "DELETE FROM agent_metrics WHERE tenant_id = $1 AND workspace_id = $2 \
     AND agent_id IN (SELECT am.agent_id FROM agent_metrics am \
       WHERE am.tenant_id = $1 AND am.workspace_id = $2 LIMIT $3)",
    "DELETE FROM workspace_context_item WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT ci.id FROM workspace_context_item ci \
       WHERE ci.tenant_id = $1 AND ci.workspace_id = $2 LIMIT $3)",
    "DELETE FROM workspace_git_root WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT r.id FROM workspace_git_root r \
       WHERE r.tenant_id = $1 AND r.workspace_id = $2 LIMIT $3)",
    "DELETE FROM pull_request_workspace WHERE tenant_id = $1 AND workspace_id = $2 \
     AND pr_key IN (SELECT pw.pr_key FROM pull_request_workspace pw \
       WHERE pw.tenant_id = $1 AND pw.workspace_id = $2 LIMIT $3)",
    "DELETE FROM workspace_invite WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT i.id FROM workspace_invite i \
       WHERE i.tenant_id = $1 AND i.workspace_id = $2 LIMIT $3)",
    "DELETE FROM draft WHERE tenant_id = $1 AND workspace_id = $2 \
     AND (agent_id, client_id) IN (SELECT dr.agent_id, dr.client_id FROM draft dr \
       WHERE dr.tenant_id = $1 AND dr.workspace_id = $2 LIMIT $3)",
    // Cascades from the workspace row in the one-shot delete; can be long.
    "DELETE FROM workspace_discussion_message WHERE tenant_id = $1 AND workspace_id = $2 \
     AND id IN (SELECT dm.id FROM workspace_discussion_message dm \
       WHERE dm.tenant_id = $1 AND dm.workspace_id = $2 LIMIT $3)",
];

/// What a `delete_purge` record deletes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PurgeKind {
    Workspace,
    Agent,
}

impl PurgeKind {
    fn as_db(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Agent => "agent",
        }
    }
}

/// Record a delete before its first batch, in the caller's writer
/// transaction (`agent` empty for a workspace delete). Already recorded (a
/// resumed or repeated delete): kept as is.
pub(super) async fn record_purge(
    tx: &mut super::WriteTx,
    tenant: &str,
    kind: PurgeKind,
    workspace: &WorkspaceId,
    agent: Option<&AgentId>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delete_purge (tenant_id, kind, workspace_id, agent_id, started_at) \
         VALUES ($1, $2, $3, $4, intent_now_iso()) ON CONFLICT DO NOTHING",
    )
    .bind(tenant)
    .bind(kind.as_db())
    .bind(&workspace.0)
    .bind(agent.map_or("", AgentId::as_str))
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

/// Drop a finished workspace delete's record, and those of its agents, in
/// the final transaction.
pub(super) async fn clear_workspace_purges(
    tx: &mut super::WriteTx,
    tenant: &str,
    workspace: &WorkspaceId,
) -> Result<()> {
    sqlx::query("DELETE FROM delete_purge WHERE tenant_id = $1 AND workspace_id = $2")
        .bind(tenant)
        .bind(&workspace.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

/// Drop a finished agent delete's record, in the final transaction.
pub(super) async fn clear_agent_purge(
    tx: &mut super::WriteTx,
    tenant: &str,
    workspace: &WorkspaceId,
    agent: &AgentId,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM delete_purge \
         WHERE tenant_id = $1 AND kind = 'agent' AND workspace_id = $2 AND agent_id = $3",
    )
    .bind(tenant)
    .bind(&workspace.0)
    .bind(&agent.0)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

impl PgStore {
    /// Drain one agent's heavy children in bounded batches.
    pub(super) async fn purge_agent_children(&self, agent: &AgentId) -> Result<()> {
        for sql in AGENT_PURGE {
            self.purge_in_batches(sql, &agent.0).await?;
        }
        Ok(())
    }

    /// Drain a workspace's heavy children, its agents' first, in bounded
    /// batches.
    pub(super) async fn purge_workspace_children(
        &self,
        workspace: &WorkspaceId,
        agents: &[AgentId],
    ) -> Result<()> {
        for agent in agents {
            self.purge_agent_children(agent).await?;
        }
        for sql in WORKSPACE_PURGE {
            self.purge_in_batches(sql, &workspace.0).await?;
        }
        Ok(())
    }

    /// Run `sql` (`$1` tenant, `$2` scope, `$3` batch size), each run its
    /// own writer transaction, until a run changes fewer rows than the
    /// batch size. The writer lock is free between runs.
    async fn purge_in_batches(&self, sql: &'static str, scope: &str) -> Result<()> {
        let limit = probe::batch_rows(self.tenant());
        loop {
            let mut tx = self.begin_write().await?;
            let changed = sqlx::query(sql)
                .bind(self.tenant())
                .bind(scope)
                .bind(limit)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
            tx.commit().await.map_err(db)?;
            if changed > 0 {
                probe::after_batch(self.tenant(), sql, changed).await?;
            }
            if changed < limit.unsigned_abs() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    /// Deletes that recorded themselves and did not finish, oldest first.
    pub(crate) async fn pending_deletes(&self) -> Result<Vec<crate::PendingDelete>> {
        let mut tx = self.begin().await?;
        let records: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT kind, workspace_id, agent_id FROM delete_purge \
             WHERE tenant_id = $1 ORDER BY started_at, kind DESC, workspace_id, agent_id",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        records
            .into_iter()
            .map(|(kind, workspace, agent)| match kind.as_str() {
                "workspace" => Ok(crate::PendingDelete::Workspace(WorkspaceId(workspace))),
                "agent" => Ok(crate::PendingDelete::Agent {
                    workspace_id: WorkspaceId(workspace),
                    agent_id: AgentId(agent),
                }),
                other => Err(Error::Internal(format!(
                    "unknown delete_purge kind {other:?}"
                ))),
            })
            .collect()
    }
}

/// Test seams for the purge: batch size, a log of batches, a pause after
/// the first batch and a simulated crash. Keyed by tenant, so parallel
/// tests on other tenants are unaffected.
#[cfg(any(test, feature = "test-support"))]
pub mod probe {
    use intent_core::{Error, Result};
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex, PoisonError};
    use tokio::sync::Notify;

    /// What one tenant's purges do differently, and what they did.
    #[derive(Default)]
    pub struct DeleteProbe {
        /// Rows per batch instead of the production size.
        pub batch_rows: Option<i64>,
        /// Each non-empty batch's table and rows deleted, in order.
        pub batches: Mutex<Vec<(String, u64)>>,
        /// After the first non-empty batch commits, notify `.0` and wait
        /// for `.1`. Taken by the first batch.
        pub pause_after_first_batch: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        /// Fail the purge, as a crash would stop it, after this many
        /// non-empty batches.
        pub fail_after_batches: Option<usize>,
    }

    impl DeleteProbe {
        /// The tables batched so far, with how many batches each took.
        #[must_use]
        pub fn batches(&self) -> Vec<(String, u64)> {
            self.batches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    static PROBES: LazyLock<Mutex<HashMap<String, Arc<DeleteProbe>>>> =
        LazyLock::new(Mutex::default);

    fn get(tenant: &str) -> Option<Arc<DeleteProbe>> {
        PROBES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(tenant)
            .cloned()
    }

    /// Install (or with `None` remove) the probe for `tenant`'s purges.
    pub fn set(tenant: &str, probe: Option<Arc<DeleteProbe>>) {
        let mut probes = PROBES.lock().unwrap_or_else(PoisonError::into_inner);
        match probe {
            Some(p) => probes.insert(tenant.to_owned(), p),
            None => probes.remove(tenant),
        };
    }

    pub(super) fn batch_rows(tenant: &str) -> i64 {
        get(tenant)
            .and_then(|p| p.batch_rows)
            .unwrap_or(super::PURGE_BATCH_ROWS)
    }

    pub(super) async fn after_batch(tenant: &str, sql: &str, rows: u64) -> Result<()> {
        let Some(probe) = get(tenant) else {
            return Ok(());
        };
        let table = sql.split_whitespace().nth(2).unwrap_or("?").to_owned();
        let done = {
            let mut batches = probe.batches.lock().unwrap_or_else(PoisonError::into_inner);
            batches.push((table, rows));
            batches.len()
        };
        let pause = probe
            .pause_after_first_batch
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
        if probe.fail_after_batches.is_some_and(|n| done >= n) {
            return Err(Error::Internal("delete probe: purge stopped".into()));
        }
        Ok(())
    }
}

#[cfg(not(any(test, feature = "test-support")))]
mod probe {
    use intent_core::Result;

    pub(super) fn batch_rows(_tenant: &str) -> i64 {
        super::PURGE_BATCH_ROWS
    }

    #[expect(clippy::unused_async, reason = "same signature as the test probe")]
    pub(super) async fn after_batch(_tenant: &str, _sql: &str, _rows: u64) -> Result<()> {
        Ok(())
    }
}
