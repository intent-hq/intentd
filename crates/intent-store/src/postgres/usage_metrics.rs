//! Change metrics and usage counters (Postgres). Mirrors `metrics_repo.rs`,
//! `usage_rate_repo.rs` and `usage_stats_repo.rs`. The usage tables
//! aggregate per tenant: one tenant's buckets never fold into another's.

use intent_core::{now_iso, Result, WorkspaceId};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore};
use crate::metrics_repo::{AgentMetricsRow, WorkspaceMetricsRow};
use crate::usage_rate_repo::{UsageRateDelta, UsageRateRow};
use crate::usage_stats_repo::{LocalStamp, UsageStatsDelta, UsageStatsRow};

/// One usage-rate reaper batch: `$1` tenant, `$2` cutoff, `$3` batch size.
const DELETE_USAGE_RATE_SQL: &str = "DELETE FROM usage_rate_minutely WHERE tenant_id = $1 \
     AND bucket_utc IN (SELECT u.bucket_utc FROM usage_rate_minutely u \
       WHERE u.tenant_id = $1 AND u.bucket_utc COLLATE \"C\" <= $2 LIMIT $3)";

impl PgStore {
    pub async fn upsert_workspace_metrics(
        &self,
        workspace_id: &WorkspaceId,
        additions: i64,
        deletions: i64,
        files_changed: i64,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO workspace_metrics \
             (tenant_id, workspace_id, additions, deletions, files_changed, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET \
             additions = excluded.additions, deletions = excluded.deletions, \
             files_changed = excluded.files_changed, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(additions)
        .bind(deletions)
        .bind(files_changed)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_workspace_metrics(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceMetricsRow>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT workspace_id, additions, deletions, files_changed, updated_at \
             FROM workspace_metrics WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(row.as_ref().map(workspace_metrics_row))
    }

    pub async fn list_workspace_metrics(&self) -> Result<Vec<WorkspaceMetricsRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT workspace_id, additions, deletions, files_changed, updated_at \
             FROM workspace_metrics WHERE tenant_id = $1 ORDER BY workspace_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(workspace_metrics_row).collect())
    }

    pub async fn delete_workspace_metrics(&self, workspace_id: &WorkspaceId) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM workspace_metrics WHERE tenant_id = $1 AND workspace_id = $2")
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn upsert_agent_metrics(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &str,
        additions: i64,
        deletions: i64,
        files_changed: i64,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_metrics \
             (tenant_id, agent_id, workspace_id, additions, deletions, files_changed, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, workspace_id, agent_id) DO UPDATE SET \
             additions = excluded.additions, deletions = excluded.deletions, \
             files_changed = excluded.files_changed, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(agent_id)
        .bind(&workspace_id.0)
        .bind(additions)
        .bind(deletions)
        .bind(files_changed)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_agent_metrics_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentMetricsRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT agent_id, workspace_id, additions, deletions, files_changed, updated_at \
             FROM agent_metrics WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY agent_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(agent_metrics_row).collect())
    }

    pub async fn list_agent_metrics(&self, agent_id: &str) -> Result<Vec<AgentMetricsRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT agent_id, workspace_id, additions, deletions, files_changed, updated_at \
             FROM agent_metrics WHERE tenant_id = $1 AND agent_id = $2 \
             ORDER BY workspace_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(agent_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(agent_metrics_row).collect())
    }

    pub async fn delete_agent_metrics_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM agent_metrics WHERE tenant_id = $1 AND workspace_id = $2")
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_agent_metrics(&self, agent_id: &str) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let removed =
            sqlx::query("DELETE FROM agent_metrics WHERE tenant_id = $1 AND agent_id = $2")
                .bind(self.tenant())
                .bind(agent_id)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }

    pub async fn add_usage_rate(&self, bucket_utc: &str, delta: &UsageRateDelta) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO usage_rate_minutely AS u (
                tenant_id, bucket_utc, input_tokens, output_tokens, cache_read_tokens,
                cache_creation_tokens, thought_tokens
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (tenant_id, bucket_utc) DO UPDATE SET
                input_tokens = u.input_tokens + excluded.input_tokens,
                output_tokens = u.output_tokens + excluded.output_tokens,
                cache_read_tokens = u.cache_read_tokens + excluded.cache_read_tokens,
                cache_creation_tokens = u.cache_creation_tokens + excluded.cache_creation_tokens,
                thought_tokens = u.thought_tokens + excluded.thought_tokens",
        )
        .bind(self.tenant())
        .bind(bucket_utc)
        .bind(delta.input_tokens.cast_signed())
        .bind(delta.output_tokens.cast_signed())
        .bind(delta.cache_read_tokens.cast_signed())
        .bind(delta.cache_creation_tokens.cast_signed())
        .bind(delta.thought_tokens.cast_signed())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_usage_rate_since(&self, since: &str) -> Result<Vec<UsageRateRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT bucket_utc, input_tokens, output_tokens, cache_read_tokens,
                    cache_creation_tokens, thought_tokens
             FROM usage_rate_minutely
             WHERE tenant_id = $1 AND bucket_utc COLLATE \"C\" >= $2
             ORDER BY bucket_utc COLLATE \"C\" ASC",
        )
        .bind(self.tenant())
        .bind(since)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|row| UsageRateRow {
                bucket_utc: row.get("bucket_utc"),
                input_tokens: counter(row, "input_tokens"),
                output_tokens: counter(row, "output_tokens"),
                cache_read_tokens: counter(row, "cache_read_tokens"),
                cache_creation_tokens: counter(row, "cache_creation_tokens"),
                thought_tokens: counter(row, "thought_tokens"),
            })
            .collect())
    }

    /// Deletes the tenant's minute buckets at or before `cutoff` in bounded
    /// batches (each its own writer transaction, see `delete_in_batches`).
    pub async fn delete_usage_rate_before(&self, cutoff: &str) -> Result<u64> {
        self.delete_in_batches(DELETE_USAGE_RATE_SQL, cutoff, super::purge::REAP_BATCH_ROWS)
            .await
    }

    pub async fn add_usage_stats(
        &self,
        bucket_utc: &str,
        model: &str,
        provider: &str,
        local: Option<&LocalStamp>,
        delta: &UsageStatsDelta,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO usage_stats_hourly AS u (
                tenant_id, bucket_utc, model, provider, local_date, local_hour, input_tokens,
                output_tokens, cache_read_tokens, cache_creation_tokens, thought_tokens, runs,
                sessions_started, longest_run_ms, lines_added, lines_deleted, tool_calls,
                tool_calls_failed, total_run_ms, sub_agent_sessions_started
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                       $17, $18, $19, $20)
             ON CONFLICT (tenant_id, bucket_utc, model, provider) DO UPDATE SET
                input_tokens = u.input_tokens + excluded.input_tokens,
                output_tokens = u.output_tokens + excluded.output_tokens,
                cache_read_tokens = u.cache_read_tokens + excluded.cache_read_tokens,
                cache_creation_tokens = u.cache_creation_tokens + excluded.cache_creation_tokens,
                thought_tokens = u.thought_tokens + excluded.thought_tokens,
                runs = u.runs + excluded.runs,
                sessions_started = u.sessions_started + excluded.sessions_started,
                longest_run_ms = GREATEST(u.longest_run_ms, excluded.longest_run_ms),
                lines_added = u.lines_added + excluded.lines_added,
                lines_deleted = u.lines_deleted + excluded.lines_deleted,
                tool_calls = u.tool_calls + excluded.tool_calls,
                tool_calls_failed = u.tool_calls_failed + excluded.tool_calls_failed,
                total_run_ms = u.total_run_ms + excluded.total_run_ms,
                sub_agent_sessions_started =
                    u.sub_agent_sessions_started + excluded.sub_agent_sessions_started",
        )
        .bind(self.tenant())
        .bind(bucket_utc)
        .bind(model)
        .bind(provider)
        .bind(local.map(|l| l.date.as_str()))
        .bind(local.map(|l| i64::from(l.hour)))
        .bind(delta.input_tokens.cast_signed())
        .bind(delta.output_tokens.cast_signed())
        .bind(delta.cache_read_tokens.cast_signed())
        .bind(delta.cache_creation_tokens.cast_signed())
        .bind(delta.thought_tokens.cast_signed())
        .bind(delta.runs.cast_signed())
        .bind(delta.sessions_started.cast_signed())
        .bind(delta.longest_run_ms.cast_signed())
        .bind(delta.lines_added.cast_signed())
        .bind(delta.lines_deleted.cast_signed())
        .bind(delta.tool_calls.cast_signed())
        .bind(delta.tool_calls_failed.cast_signed())
        .bind(delta.total_run_ms.cast_signed())
        .bind(delta.sub_agent_sessions_started.cast_signed())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_usage_stats_hourly(&self) -> Result<Vec<UsageStatsRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT bucket_utc, model, provider, local_date, local_hour, input_tokens,
                    output_tokens, cache_read_tokens, cache_creation_tokens, thought_tokens, runs,
                    sessions_started, longest_run_ms, lines_added, lines_deleted, tool_calls,
                    tool_calls_failed, total_run_ms, sub_agent_sessions_started
             FROM usage_stats_hourly
             WHERE tenant_id = $1
             ORDER BY bucket_utc COLLATE \"C\" ASC, model COLLATE \"C\" ASC,
                      provider COLLATE \"C\" ASC",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|row| UsageStatsRow {
                bucket_utc: row.get("bucket_utc"),
                model: row.get("model"),
                provider: row.get("provider"),
                local_date: row.get("local_date"),
                local_hour: row
                    .get::<Option<i64>, _>("local_hour")
                    .and_then(|h| u8::try_from(h).ok())
                    .filter(|h| *h < 24),
                input_tokens: counter(row, "input_tokens"),
                output_tokens: counter(row, "output_tokens"),
                cache_read_tokens: counter(row, "cache_read_tokens"),
                cache_creation_tokens: counter(row, "cache_creation_tokens"),
                thought_tokens: counter(row, "thought_tokens"),
                runs: counter(row, "runs"),
                sessions_started: counter(row, "sessions_started"),
                longest_run_ms: counter(row, "longest_run_ms"),
                lines_added: counter(row, "lines_added"),
                lines_deleted: counter(row, "lines_deleted"),
                tool_calls: counter(row, "tool_calls"),
                tool_calls_failed: counter(row, "tool_calls_failed"),
                total_run_ms: counter(row, "total_run_ms"),
                sub_agent_sessions_started: counter(row, "sub_agent_sessions_started"),
            })
            .collect())
    }
}

fn counter(row: &PgRow, column: &str) -> u64 {
    row.get::<i64, _>(column).cast_unsigned()
}

fn workspace_metrics_row(r: &PgRow) -> WorkspaceMetricsRow {
    WorkspaceMetricsRow {
        workspace_id: WorkspaceId(r.get("workspace_id")),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        files_changed: r.get("files_changed"),
        updated_at: r.get("updated_at"),
    }
}

fn agent_metrics_row(r: &PgRow) -> AgentMetricsRow {
    AgentMetricsRow {
        agent_id: r.get("agent_id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        files_changed: r.get("files_changed"),
        updated_at: r.get("updated_at"),
    }
}
