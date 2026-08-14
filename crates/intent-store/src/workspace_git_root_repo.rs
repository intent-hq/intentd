//! Workspace-git-root repository: CRUD for secondary git repositories tracked
//! for a workspace (multi git root tracking, intent-hq/monorepo#2053).
//! Registration is idempotent by `(workspace_id, path)` — the upsert appends
//! the registering agent ids (deduped) to the existing row instead of
//! duplicating it. Rows cascade with their workspace (FK `ON DELETE CASCADE`).

use intent_core::{
    AgentId, Error, PullRequestInfo, PullRequestStatus, Result, WorkspaceGitRoot,
    WorkspaceGitRootId, WorkspaceGitRootSource, WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, Store};

const COLUMNS: &str = "id, workspace_id, path, source, repo_owner, repo_name, \
    registered_by_agent_ids, pr_number, pr_url, pr_status, pull_requests, created_at, updated_at";

/// Encode the `registered_by_agent_ids` list to its JSON-array TEXT column.
fn agent_ids_to_db(ids: &[AgentId]) -> Result<String> {
    serde_json::to_string(ids)
        .map_err(|e| Error::Internal(format!("encode registered_by_agent_ids failed: {e}")))
}

/// Decode the `registered_by_agent_ids` JSON-array TEXT column.
fn agent_ids_from_db(s: &str) -> Result<Vec<AgentId>> {
    serde_json::from_str(s)
        .map_err(|e| Error::Internal(format!("decode registered_by_agent_ids failed: {e}")))
}

/// Encode the optional `pull_requests` snapshot list to a JSON TEXT column.
fn pull_requests_to_db(prs: Option<&Vec<PullRequestInfo>>) -> Result<Option<String>> {
    prs.map(|prs| {
        serde_json::to_string(prs)
            .map_err(|e| Error::Internal(format!("encode pull_requests failed: {e}")))
    })
    .transpose()
}

/// Decode the optional `pull_requests` JSON TEXT column.
fn pull_requests_from_db(s: Option<String>) -> Result<Option<Vec<PullRequestInfo>>> {
    s.map(|json| {
        serde_json::from_str::<Vec<PullRequestInfo>>(&json)
            .map_err(|e| Error::Internal(format!("decode pull_requests failed: {e}")))
    })
    .transpose()
}

fn root_from_row(r: &SqliteRow) -> Result<WorkspaceGitRoot> {
    let err = |e: sqlx::Error| Error::Internal(format!("read workspace git root row: {e}"));
    let get = |col: &str| -> Result<String> { r.try_get::<String, _>(col).map_err(err) };
    let get_opt =
        |col: &str| -> Result<Option<String>> { r.try_get::<Option<String>, _>(col).map_err(err) };
    let pr_status = get_opt("pr_status")?
        .map(|s| enum_from_db::<PullRequestStatus>(&s))
        .transpose()?;
    Ok(WorkspaceGitRoot {
        id: WorkspaceGitRootId(get("id")?),
        workspace_id: WorkspaceId(get("workspace_id")?),
        path: get("path")?,
        source: enum_from_db::<WorkspaceGitRootSource>(&get("source")?)?,
        repo_owner: get_opt("repo_owner")?,
        repo_name: get_opt("repo_name")?,
        registered_by_agent_ids: agent_ids_from_db(&get("registered_by_agent_ids")?)?,
        pr_number: r
            .try_get::<Option<i64>, _>("pr_number")
            .map_err(err)?
            .map(|n| n as u64),
        pr_url: get_opt("pr_url")?,
        pr_status,
        pull_requests: pull_requests_from_db(get_opt("pull_requests")?)?,
        created_at: get("created_at")?,
        updated_at: get("updated_at")?,
    })
}

impl Store {
    /// Insert-or-merge a git root, idempotent by `(workspace_id, path)`.
    ///
    /// When no row exists for the pair, `root` is inserted as-is (with its
    /// `registered_by_agent_ids` deduped). When a row already exists, it is
    /// merged in place and the merged row returned: `root`'s agent ids are
    /// appended (deduped, registration order preserved), `repo_owner` /
    /// `repo_name` are refreshed when `root` carries them (kept otherwise),
    /// `source` is upgraded `auto` → `agent` when `root` is agent-registered
    /// (never downgraded — an explicit registration takes over an
    /// auto-detected row in place, monorepo#2053), and `updated_at` is taken
    /// from `root`. The existing row's `id`, PR fields, and `created_at` are
    /// retained.
    ///
    /// Returns the stored row plus `true` when it was newly inserted /
    /// `false` when an existing row was merged. The flag is decided inside
    /// the same serialized write transaction as the insert-or-merge itself,
    /// so concurrent registrations of one path observe exactly one insert.
    pub async fn upsert_workspace_git_root(
        &self,
        root: &WorkspaceGitRoot,
    ) -> Result<(WorkspaceGitRoot, bool)> {
        let pool = self.write_pool();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("upsert workspace git root tx failed: {e}"))
            })?;
            let sql = format!(
                "SELECT {COLUMNS} FROM workspace_git_root WHERE workspace_id = ? AND path = ?"
            );
            let existing = sqlx::query(&sql)
                .bind(&root.workspace_id.0)
                .bind(&root.path)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("find workspace git root failed: {e}")))?;
            let (merged, inserted) = match existing {
                Some(row) => {
                    let mut current = root_from_row(&row)?;
                    for id in &root.registered_by_agent_ids {
                        if !current.registered_by_agent_ids.contains(id) {
                            current.registered_by_agent_ids.push(id.clone());
                        }
                    }
                    if root.repo_owner.is_some() {
                        current.repo_owner = root.repo_owner.clone();
                    }
                    if root.repo_name.is_some() {
                        current.repo_name = root.repo_name.clone();
                    }
                    if current.source == WorkspaceGitRootSource::Auto
                        && root.source == WorkspaceGitRootSource::Agent
                    {
                        current.source = WorkspaceGitRootSource::Agent;
                    }
                    current.updated_at = root.updated_at.clone();
                    sqlx::query(
                        "UPDATE workspace_git_root SET registered_by_agent_ids = ?, \
                         repo_owner = ?, repo_name = ?, source = ?, updated_at = ? WHERE id = ?",
                    )
                    .bind(agent_ids_to_db(&current.registered_by_agent_ids)?)
                    .bind(&current.repo_owner)
                    .bind(&current.repo_name)
                    .bind(enum_to_db(&current.source)?)
                    .bind(&current.updated_at)
                    .bind(&current.id.0)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("merge workspace git root failed: {e}"))
                    })?;
                    (current, false)
                }
                None => {
                    let mut fresh = root.clone();
                    let mut seen: Vec<AgentId> = Vec::new();
                    fresh.registered_by_agent_ids.retain(|id| {
                        if seen.contains(id) {
                            false
                        } else {
                            seen.push(id.clone());
                            true
                        }
                    });
                    let sql = format!(
                        "INSERT INTO workspace_git_root ({COLUMNS}) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    );
                    sqlx::query(&sql)
                        .bind(&fresh.id.0)
                        .bind(&fresh.workspace_id.0)
                        .bind(&fresh.path)
                        .bind(enum_to_db(&fresh.source)?)
                        .bind(&fresh.repo_owner)
                        .bind(&fresh.repo_name)
                        .bind(agent_ids_to_db(&fresh.registered_by_agent_ids)?)
                        .bind(fresh.pr_number.map(|n| n as i64))
                        .bind(&fresh.pr_url)
                        .bind(fresh.pr_status.map(|s| enum_to_db(&s)).transpose()?)
                        .bind(pull_requests_to_db(fresh.pull_requests.as_ref())?)
                        .bind(&fresh.created_at)
                        .bind(&fresh.updated_at)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("insert workspace git root failed: {e}"))
                        })?;
                    (fresh, true)
                }
            };
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("upsert workspace git root commit failed: {e}"))
            })?;
            Ok((merged, inserted))
        })
        .await
    }

    /// Fetch a single git root by id, or `NotFound`.
    pub async fn get_workspace_git_root(
        &self,
        id: &WorkspaceGitRootId,
    ) -> Result<WorkspaceGitRoot> {
        let sql = format!("SELECT {COLUMNS} FROM workspace_git_root WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace git root failed: {e}")))?;
        match row {
            Some(r) => root_from_row(&r),
            None => Err(Error::NotFound(format!("workspace git root {id}"))),
        }
    }

    /// The git root registered for `(workspace_id, path)`, if any.
    pub async fn find_workspace_git_root_by_path(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
    ) -> Result<Option<WorkspaceGitRoot>> {
        let sql =
            format!("SELECT {COLUMNS} FROM workspace_git_root WHERE workspace_id = ? AND path = ?");
        let row = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(path)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("find workspace git root failed: {e}")))?;
        row.as_ref().map(root_from_row).transpose()
    }

    /// Every git root registered for a workspace, oldest first.
    pub async fn list_workspace_git_roots(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceGitRoot>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM workspace_git_root WHERE workspace_id = ? ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspace git roots failed: {e}")))?;
        rows.iter().map(root_from_row).collect()
    }

    /// Delete a git root by id; `NotFound` when absent.
    pub async fn delete_workspace_git_root(&self, id: &WorkspaceGitRootId) -> Result<()> {
        let res = sqlx::query("DELETE FROM workspace_git_root WHERE id = ?")
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace git root failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace git root {id}")));
        }
        Ok(())
    }

    /// Update only the PR-linkage fields of a git root (`pr_number`,
    /// `pr_url`, `pr_status`, `pull_requests`) plus `updated_at`, from the
    /// in-memory entity — the scoped write the background PR-discovery sweep
    /// uses (mirrors [`Store::update_workspace_pr_linkage`]). `NotFound` when
    /// the row is absent.
    pub async fn update_workspace_git_root_pr(&self, root: &WorkspaceGitRoot) -> Result<()> {
        let res = sqlx::query(
            "UPDATE workspace_git_root SET pr_number = ?, pr_url = ?, pr_status = ?, \
             pull_requests = ?, updated_at = ? WHERE id = ?",
        )
        .bind(root.pr_number.map(|n| n as i64))
        .bind(&root.pr_url)
        .bind(root.pr_status.map(|s| enum_to_db(&s)).transpose()?)
        .bind(pull_requests_to_db(root.pull_requests.as_ref())?)
        .bind(&root.updated_at)
        .bind(&root.id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace git root pr failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace git root {}", root.id)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    use uuid::Uuid;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("test-git-root-{}.db", Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut sidecar = self.path.clone().into_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(&sidecar);
            }
        }
    }

    fn test_workspace(ws_id: &WorkspaceId, ts: &str) -> Workspace {
        Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    /// Open a store with one workspace (the FK target a `workspace_git_root`
    /// row needs) and return them.
    async fn store_with_workspace() -> (TempDb, Store, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ts = now_iso();
        let ws_id = WorkspaceId(format!("ws-{}", Uuid::new_v4()));
        store
            .insert_workspace(&test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        (tmp, store, ws_id)
    }

    fn test_root(ws_id: &WorkspaceId, path: &str, agents: &[&str], ts: &str) -> WorkspaceGitRoot {
        WorkspaceGitRoot {
            id: WorkspaceGitRootId::new(),
            workspace_id: ws_id.clone(),
            path: path.to_string(),
            source: WorkspaceGitRootSource::Agent,
            repo_owner: None,
            repo_name: None,
            registered_by_agent_ids: agents.iter().map(|a| AgentId(a.to_string())).collect(),
            pr_number: None,
            pr_url: None,
            pr_status: None,
            pull_requests: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
        }
    }

    /// A first upsert inserts (`inserted == true`); a second upsert on the
    /// same `(workspace, path)` merges into the existing row (`inserted ==
    /// false`) — the new agent is appended, the already-registered agent is
    /// not duplicated, the row keeps its `id` / `source` / `created_at`, and
    /// no second row appears.
    #[tokio::test]
    async fn upsert_appends_registering_agent_deduped() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let ts = now_iso();
        let first = test_root(&ws_id, "/tmp/clone-a", &["agent-1"], &ts);
        let (inserted, was_inserted) = store
            .upsert_workspace_git_root(&first)
            .await
            .expect("insert");
        assert!(was_inserted, "first upsert inserts");
        assert_eq!(inserted.id, first.id);
        assert_eq!(
            inserted.registered_by_agent_ids,
            vec![AgentId("agent-1".into())]
        );

        let later = now_iso();
        let mut second = test_root(&ws_id, "/tmp/clone-a", &["agent-1", "agent-2"], &later);
        second.repo_owner = Some("intent-hq".to_string());
        second.repo_name = Some("intentd".to_string());
        let (merged, was_inserted) = store
            .upsert_workspace_git_root(&second)
            .await
            .expect("merge");
        assert!(!was_inserted, "second upsert merges");
        assert_eq!(merged.id, first.id, "existing row id retained");
        assert_eq!(merged.created_at, first.created_at, "created_at retained");
        assert_eq!(
            merged.registered_by_agent_ids,
            vec![AgentId("agent-1".into()), AgentId("agent-2".into())],
            "second agent appended, first not duplicated"
        );
        assert_eq!(merged.repo_owner.as_deref(), Some("intent-hq"));
        assert_eq!(merged.repo_name.as_deref(), Some("intentd"));

        let listed = store.list_workspace_git_roots(&ws_id).await.expect("list");
        assert_eq!(listed.len(), 1, "idempotent by (workspace, path)");
        assert_eq!(listed[0], merged);
    }

    /// A merge upsert without owner/name keeps the persisted detection, and
    /// a fresh insert dedupes a repeated agent id within the candidate list.
    #[tokio::test]
    async fn upsert_keeps_detection_and_dedupes_candidate() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let ts = now_iso();
        let mut first = test_root(&ws_id, "/tmp/clone-b", &["agent-1", "agent-1"], &ts);
        first.repo_owner = Some("intent-hq".to_string());
        first.repo_name = Some("intentd".to_string());
        let (inserted, _) = store
            .upsert_workspace_git_root(&first)
            .await
            .expect("insert");
        assert_eq!(
            inserted.registered_by_agent_ids,
            vec![AgentId("agent-1".into())],
            "candidate list deduped on insert"
        );

        let (merged, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/clone-b", &[], &now_iso()))
            .await
            .expect("merge");
        assert_eq!(merged.repo_owner.as_deref(), Some("intent-hq"));
        assert_eq!(merged.repo_name.as_deref(), Some("intentd"));
        assert_eq!(
            merged.registered_by_agent_ids,
            vec![AgentId("agent-1".into())]
        );
    }

    /// A merge upsert upgrades `source` `auto` → `agent` when the candidate
    /// is agent-registered, and never downgrades an `agent` row back to
    /// `auto` (monorepo#2053).
    #[tokio::test]
    async fn upsert_upgrades_source_auto_to_agent_never_downgrades() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let mut auto_root = test_root(&ws_id, "/tmp/clone-c", &[], &now_iso());
        auto_root.source = WorkspaceGitRootSource::Auto;
        let (inserted, _) = store
            .upsert_workspace_git_root(&auto_root)
            .await
            .expect("insert");
        assert_eq!(inserted.source, WorkspaceGitRootSource::Auto);

        // An agent registration takes over the auto-detected row in place.
        let (upgraded, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/clone-c", &["agent-1"], &now_iso()))
            .await
            .expect("upgrade");
        assert_eq!(upgraded.id, inserted.id, "row taken over in place");
        assert_eq!(upgraded.source, WorkspaceGitRootSource::Agent);

        // A later auto-detection pass must not downgrade it back.
        let mut auto_again = test_root(&ws_id, "/tmp/clone-c", &[], &now_iso());
        auto_again.source = WorkspaceGitRootSource::Auto;
        let (kept, _) = store
            .upsert_workspace_git_root(&auto_again)
            .await
            .expect("merge");
        assert_eq!(kept.source, WorkspaceGitRootSource::Agent, "no downgrade");
    }

    /// get/list/find/delete round-trip: roots list oldest-first and scoped to
    /// their workspace, `find` resolves by path, and a deleted root is gone
    /// (second delete → `NotFound`).
    #[tokio::test]
    async fn get_list_find_delete_round_trip() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let (a, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/a", &["agent-1"], &now_iso()))
            .await
            .expect("insert a");
        let (b, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/b", &[], "2999-01-01T00:00:00Z"))
            .await
            .expect("insert b");

        assert_eq!(store.get_workspace_git_root(&a.id).await.expect("get"), a);
        let listed = store.list_workspace_git_roots(&ws_id).await.expect("list");
        assert_eq!(listed, vec![a.clone(), b.clone()], "oldest first");
        assert_eq!(
            store
                .find_workspace_git_root_by_path(&ws_id, "/tmp/b")
                .await
                .expect("find"),
            Some(b)
        );
        assert_eq!(
            store
                .find_workspace_git_root_by_path(&ws_id, "/tmp/absent")
                .await
                .expect("find absent"),
            None
        );

        store
            .delete_workspace_git_root(&a.id)
            .await
            .expect("delete");
        assert!(matches!(
            store.delete_workspace_git_root(&a.id).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            store.get_workspace_git_root(&a.id).await,
            Err(Error::NotFound(_))
        ));
    }

    /// `update_workspace_git_root_pr` writes only the PR fields and they
    /// round-trip, including the serialized `pull_requests` list; an absent
    /// row is `NotFound`.
    #[tokio::test]
    async fn pr_fields_update_round_trip() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let (mut root, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/pr", &["agent-1"], &now_iso()))
            .await
            .expect("insert");

        root.pr_number = Some(7);
        root.pr_url = Some("https://github.com/o/r/pull/7".to_string());
        root.pr_status = Some(PullRequestStatus::Open);
        root.pull_requests = Some(vec![PullRequestInfo {
            id: "PR_1".to_string(),
            number: 7,
            url: "https://github.com/o/r/pull/7".to_string(),
            title: "feat: x".to_string(),
            status: PullRequestStatus::Open,
            created_at: root.created_at.clone(),
            updated_at: root.created_at.clone(),
            base_ref: Some("main".to_string()),
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: None,
            mergeable_state: None,
            is_draft: None,
        }]);
        root.updated_at = now_iso();
        store
            .update_workspace_git_root_pr(&root)
            .await
            .expect("update pr fields");

        let read = store.get_workspace_git_root(&root.id).await.expect("get");
        assert_eq!(read, root);

        let mut absent = root.clone();
        absent.id = WorkspaceGitRootId::new();
        assert!(matches!(
            store.update_workspace_git_root_pr(&absent).await,
            Err(Error::NotFound(_))
        ));
    }

    /// Deleting the owning workspace cascades its git-root rows (FK
    /// `ON DELETE CASCADE`).
    #[tokio::test]
    async fn workspace_delete_cascades_git_roots() {
        let (_tmp, store, ws_id) = store_with_workspace().await;
        let (root, _) = store
            .upsert_workspace_git_root(&test_root(&ws_id, "/tmp/cascade", &[], &now_iso()))
            .await
            .expect("insert");

        store.delete_workspace(&ws_id).await.expect("delete ws");
        assert!(matches!(
            store.get_workspace_git_root(&root.id).await,
            Err(Error::NotFound(_))
        ));
        assert!(store
            .list_workspace_git_roots(&ws_id)
            .await
            .expect("list after cascade")
            .is_empty());
    }
}
