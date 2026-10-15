//! A reserved `SQLite` writer, with no authority reconstructed from stored data.
use super::{
    kind_word, pending_to_db, provider_word, state_to_db, validate_monitor_target,
    MonitorTargetProvenance, PrMonitor, QualifiedPrMonitor, QueryBuilder, Result, ReviewTarget,
    Sqlite, Store, COLUMNS, TARGET_COLUMNS,
};
use sqlx::Transaction;

/// One prebuilt monitor mutation on an already reserved database writer.
/// Dropping it before execution rolls back its empty transaction. Application
/// code must retain original authority through `commit`, not just preparation.
/// No pool acquisition, provider call or application callback occurs in commit.
pub struct PreparedMonitorWrite {
    transaction: Transaction<'static, Sqlite>,
    query: QueryBuilder<'static, Sqlite>,
    insert: bool,
}

impl PreparedMonitorWrite {
    /// Execute and finish while the caller's original authority fences are held.
    /// This synchronous method is only for a blocking worker: it waits for the
    /// reserved `SQLite` worker through COMMIT (or rollback on failure). Dropping
    /// the async caller cannot cancel a commit that has already been admitted.
    ///
    /// # Errors
    /// Returns a database error; uniqueness/CAS conflicts return false.
    /// # Panics
    /// Panics if called directly on an asynchronous runtime worker.
    pub fn commit(self) -> Result<bool> {
        tokio::runtime::Handle::current().block_on(self.finish())
    }

    async fn finish(mut self) -> Result<bool> {
        let result = self.query.build().execute(&mut *self.transaction).await;
        let changed = match result {
            Ok(result) => result.rows_affected() == 1,
            Err(error)
                if self.insert
                    && error
                        .as_database_error()
                        .is_some_and(sqlx::error::DatabaseError::is_unique_violation) =>
            {
                false
            }
            Err(error) => {
                return Err(intent_core::Error::Internal(format!(
                    "qualified monitor write: {error}"
                )))
            }
        };
        self.transaction.commit().await.map_err(|error| {
            intent_core::Error::Internal(format!("qualified monitor commit: {error}"))
        })?;
        Ok(changed)
    }
}

impl Store {
    async fn prepare_monitor_write(
        &self,
        query: QueryBuilder<'static, Sqlite>,
        insert: bool,
    ) -> Result<PreparedMonitorWrite> {
        // Acquire SQLite's write reservation before caller/account fences. A
        // competing lifecycle writer can retire its leaf without waiting for
        // this operation to obtain the writer or another application lock.
        let transaction = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| {
                intent_core::Error::Internal(format!("prepare monitor write: {error}"))
            })?;
        Ok(PreparedMonitorWrite {
            transaction,
            query,
            insert,
        })
    }

    /// Prepare a canonical insert without executing it or granting authority.
    /// # Errors
    /// Returns invalid target or database acquisition errors.
    pub async fn prepare_monitor_insert(
        &self,
        m: &PrMonitor,
        target: &ReviewTarget,
    ) -> Result<PreparedMonitorWrite> {
        validate_monitor_target(m, target, MonitorTargetProvenance::CapturedRequest)?;
        let mut query = QueryBuilder::new(format!(
            "INSERT INTO pr_monitor ({COLUMNS}, {TARGET_COLUMNS}) VALUES ("
        ));
        let mut values = query.separated(",");
        values
            .push_bind(m.monitor_id.0.clone())
            .push_bind(m.workspace_id.0.clone())
            .push_bind(m.agent_id.0.clone())
            .push_bind(m.repo_owner.clone())
            .push_bind(m.repo_name.clone())
            .push_bind(m.pr_number)
            .push_bind(state_to_db(m.state))
            .push_bind(m.last_snapshot.clone())
            .push_bind(m.baseline_snapshot.clone())
            .push_bind(pending_to_db(&m.pending_changes))
            .push_bind(m.pending_since.clone())
            .push_bind(m.last_change_at.clone())
            .push_bind(m.last_polled_at.clone())
            .push_bind(m.last_error.clone())
            .push_bind(m.created_at.clone())
            .push_bind(m.updated_at.clone())
            .push_bind(provider_word(target.repository.provider))
            .push_bind(target.repository.instance_base_url.clone())
            .push_bind(target.repository.project_path.clone())
            .push_bind(kind_word(target.kind))
            .push_bind(MonitorTargetProvenance::CapturedRequest.as_str())
            .push_bind(None::<String>);
        query.push(")");
        self.prepare_monitor_write(query, true).await
    }

    /// Prepare a refresh, lifecycle change or ownership transfer of the exact captured row.
    /// # Errors
    /// Returns inconsistent identity or database acquisition errors.
    pub async fn prepare_monitor_replace(
        &self,
        expected: &QualifiedPrMonitor,
        replacement: &PrMonitor,
    ) -> Result<PreparedMonitorWrite> {
        let old = expected.monitor();
        if old.monitor_id != replacement.monitor_id
            || old.workspace_id != replacement.workspace_id
            || old.repo_owner != replacement.repo_owner
            || old.repo_name != replacement.repo_name
            || old.pr_number != replacement.pr_number
            || old.created_at != replacement.created_at
        {
            return Err(intent_core::Error::InvalidParams(
                "monitor replacement changed resource identity".into(),
            ));
        }
        let mut query = QueryBuilder::new("UPDATE pr_monitor SET agent_id = ");
        query
            .push_bind(replacement.agent_id.0.clone())
            .push(", state = ")
            .push_bind(state_to_db(replacement.state))
            .push(", last_snapshot = ")
            .push_bind(replacement.last_snapshot.clone())
            .push(", baseline_snapshot = ")
            .push_bind(replacement.baseline_snapshot.clone())
            .push(", pending_changes = ")
            .push_bind(pending_to_db(&replacement.pending_changes))
            .push(", pending_since = ")
            .push_bind(replacement.pending_since.clone())
            .push(", last_change_at = ")
            .push_bind(replacement.last_change_at.clone())
            .push(", last_polled_at = ")
            .push_bind(replacement.last_polled_at.clone())
            .push(", last_error = ")
            .push_bind(replacement.last_error.clone())
            .push(", updated_at = ")
            .push_bind(replacement.updated_at.clone());
        captured_condition(&mut query, expected);
        self.prepare_monitor_write(query, false).await
    }

    /// Prepare cancellation without losing the complete captured-row CAS.
    /// # Errors
    /// Returns database acquisition errors.
    pub async fn prepare_monitor_cancel(
        &self,
        expected: &QualifiedPrMonitor,
        now: &str,
    ) -> Result<PreparedMonitorWrite> {
        let mut query =
            QueryBuilder::new("UPDATE pr_monitor SET state = 'cancelled', updated_at = ");
        query.push_bind(now.to_owned());
        captured_condition(&mut query, expected);
        self.prepare_monitor_write(query, false).await
    }
}

/// Both the legacy Store CAS entry and prepared writes compare original bytes.
pub(super) fn captured_condition(
    query: &mut QueryBuilder<'static, Sqlite>,
    expected: &QualifiedPrMonitor,
) {
    query.push(" WHERE state = 'active'");
    for (i, column) in COLUMNS.split(',').map(str::trim).enumerate() {
        query.push(" AND ").push(column).push(" IS ");
        if column == "pr_number" {
            query.push_bind(expected.monitor.pr_number);
        } else {
            query.push_bind(expected.original_text[i].clone());
        }
    }
    for (column, value) in TARGET_COLUMNS
        .split(',')
        .map(str::trim)
        .zip(expected.columns.values())
    {
        query
            .push(" AND ")
            .push(column)
            .push(" IS ")
            .push_bind(value.map(str::to_owned));
    }
}
