//! Loaded workspace deletion regressions (intent-hq/intent#5337).

use super::{sample_agent_session, sample_workspace, TempDb};
use crate::Store;
use intent_core::{
    AgentId, BrowserTabInput, BrowserTabVisibility, ClientHostInfo, ClientId, Error, WorkspaceId,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const HISTORY_ROWS: i64 = 1_201;
const PRESTAGED_ROWS: i64 = 7;

async fn seed_history(store: &Store, workspace: &WorkspaceId, name: &str, count: i64) -> AgentId {
    let agent = AgentId::from(name);
    store
        .insert_agent_session(&sample_agent_session(&agent, workspace))
        .await
        .expect("seed session");
    let mut tx = store
        .write_pool()
        .begin()
        .await
        .expect("begin history seed");
    sqlx::query(
        "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM rows WHERE n < ?) \
         INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
         SELECT ? || '-' || n, ?, n, 'assistant', \
                '[{\"type\":\"text\",\"text\":\"searchable transcript\"}]', 't0' FROM rows",
    )
    .bind(count)
    .bind(name)
    .bind(name)
    .execute(&mut *tx)
    .await
    .expect("seed searchable messages");
    sqlx::query(
        "INSERT INTO agent_message_payload \
         (message_id, agent_id, block_ordinal, kind, encoding, body) \
         SELECT id, agent_id, 0, 'tool_result_output', 'none', zeroblob(8192) \
         FROM agent_message WHERE agent_id = ?",
    )
    .bind(name)
    .execute(&mut *tx)
    .await
    .expect("seed message payloads");
    for n in 0..PRESTAGED_ROWS {
        sqlx::query(
            "INSERT INTO agent_message_payload \
             (message_id, agent_id, block_ordinal, kind, encoding, body) \
             VALUES (?, ?, 0, 'tool_use_input', 'none', zeroblob(8192))",
        )
        .bind(format!("{name}-prestaged-{n}"))
        .bind(name)
        .execute(&mut *tx)
        .await
        .expect("seed pre-staged payload");
    }
    tx.commit().await.expect("commit history seed");
    agent
}

async fn seed_workspace(store: &Store, id: &str) -> WorkspaceId {
    let id = WorkspaceId::from(id);
    store
        .insert_workspace(&sample_workspace(&id, id.as_str(), false))
        .await
        .expect("seed workspace");
    id
}

async fn count_history(store: &Store, workspace: &WorkspaceId) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM agent_message m JOIN agent_session s ON s.id = m.agent_id \
                 WHERE s.workspace_id = ?) + \
                (SELECT COUNT(*) FROM agent_message_payload p JOIN agent_session s ON s.id = p.agent_id \
                 WHERE s.workspace_id = ?)",
    )
    .bind(&workspace.0)
    .bind(&workspace.0)
    .fetch_one(store.read_pool())
    .await
    .expect("count history")
}

/// Both futures contend on the SAME single-connection writer pool. The
/// observer keeps queuing a transaction behind deletion: the pool's fair
/// acquisition queue lets it see every committed cleanup step. A root
/// cascade only exposes all-history or no-history, never partial progress.
/// The deadline is a hang guard, not a responsiveness assertion.
#[tokio::test]
async fn loaded_workspace_delete_releases_writer_during_history_sweep() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    for n in 0..3 {
        seed_history(&store, &doomed, &format!("doomed-{n}"), HISTORY_ROWS).await;
    }
    seed_history(&store, &keeper, "kept-agent", 3).await;
    let initial = count_history(&store, &doomed).await;
    assert_eq!(initial, 3 * (2 * HISTORY_ROWS + PRESTAGED_ROWS));
    let finished = AtomicBool::new(false);
    let deleting = async {
        let result = store.delete_workspace(&doomed).await;
        finished.store(true, Ordering::SeqCst);
        result
    };
    let writing = async {
        let mut observed_progress = false;
        let mut observed_session_progress = false;
        while !finished.load(Ordering::SeqCst) {
            let mut tx = store.write_pool().begin().await.expect("unrelated writer");
            let remaining: i64 = sqlx::query_scalar(
                "SELECT (SELECT COUNT(*) FROM agent_message m JOIN agent_session s ON s.id = m.agent_id \
                         WHERE s.workspace_id = ?) + \
                        (SELECT COUNT(*) FROM agent_message_payload p JOIN agent_session s ON s.id = p.agent_id \
                         WHERE s.workspace_id = ?)",
            )
            .bind(&doomed.0)
            .bind(&doomed.0)
            .fetch_one(&mut *tx)
            .await
            .expect("observe committed cleanup");
            let partial = remaining > 0 && remaining < initial;
            let sessions: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE workspace_id = ?")
                    .bind(&doomed.0)
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            if partial {
                let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workspace WHERE id = ?")
                    .bind(&doomed.0)
                    .fetch_one(&mut *tx)
                    .await
                    .expect("workspace remains during sweep");
                assert_eq!(live, 1);
                let tombstones: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id WHERE id = ?")
                        .bind(&doomed.0)
                        .fetch_one(&mut *tx)
                        .await
                        .expect("no early tombstone");
                assert_eq!(tombstones, 0);
            }
            sqlx::query("UPDATE workspace SET title = 'unrelated write committed' WHERE id = ?")
                .bind(&keeper.0)
                .execute(&mut *tx)
                .await
                .expect("unrelated write");
            tx.commit().await.expect("unrelated commit");
            observed_progress |= partial;
            observed_session_progress |= sessions > 0 && sessions < 3;
        }
        (observed_progress, observed_session_progress)
    };
    let (deleted, (observed_progress, observed_session_progress)) =
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(deleting, writing)
        })
        .await
        .expect("delete and writer finish");
    deleted.expect("workspace delete");
    assert!(
        observed_progress,
        "another writer must commit after history cleanup begins and before it finishes"
    );
    assert!(
        observed_session_progress,
        "another writer commits between session deletions"
    );
    assert_eq!(count_history(&store, &keeper).await, 2 * 3 + PRESTAGED_ROWS);
    assert_eq!(count_history(&store, &doomed).await, 0);
    assert_deleted(&store, &doomed, 3).await;
}

async fn assert_deleted(store: &Store, workspace: &WorkspaceId, kept_messages: i64) {
    assert!(matches!(
        store.get_workspace(workspace).await,
        Err(Error::NotFound(_))
    ));
    let tombstones: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id WHERE id = ?")
            .bind(&workspace.0)
            .fetch_one(store.read_pool())
            .await
            .expect("tombstone count");
    assert_eq!(tombstones, 1);
    let sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE workspace_id = ?")
            .bind(&workspace.0)
            .fetch_one(store.read_pool())
            .await
            .expect("session count");
    assert_eq!(sessions, 0);
    for table in [
        "agent_message",
        "agent_message_fts",
        "agent_message_search_ctx",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .expect("message/search count");
        assert_eq!(
            count, kept_messages,
            "{table} preserves only unrelated rows"
        );
    }
    let matches: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_message_fts WHERE agent_message_fts MATCH 'searchable'",
    )
    .fetch_one(store.read_pool())
    .await
    .expect("search remaining messages");
    assert_eq!(matches, kept_messages);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(store.read_pool())
        .await
        .expect("foreign key check")
        .is_empty());
}

/// A real `SQLite` error in the second message batch must leave the first
/// batch committed, the workspace live, and no tombstone. Removing the fault
/// and retrying must tolerate a session removed separately in the meantime.
#[tokio::test]
async fn workspace_delete_failure_preserves_progress_and_can_retry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    let interrupted = seed_history(&store, &doomed, "interrupted", HISTORY_ROWS).await;
    let removed = seed_history(&store, &doomed, "removed-separately", HISTORY_ROWS).await;
    seed_history(&store, &keeper, "kept-agent", 3).await;
    sqlx::query(
        "CREATE TRIGGER fail_message_cleanup BEFORE DELETE ON agent_message \
         WHEN OLD.agent_id = 'interrupted' AND \
              (SELECT COUNT(*) FROM agent_message WHERE agent_id = OLD.agent_id) <= 701 \
         BEGIN SELECT RAISE(ABORT, 'injected message cleanup failure'); END",
    )
    .execute(store.write_pool())
    .await
    .expect("inject failure after one committed batch");
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::Internal(message)) if message.contains("injected message cleanup failure")
    ));
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_message WHERE agent_id = ?")
            .bind(&interrupted.0)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(remaining, 701, "the first 500 messages stay deleted");
    let payloads: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_message_payload WHERE agent_id = ?")
            .bind(&interrupted.0)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(payloads, 0, "the payload sweep stays committed");
    store
        .get_workspace(&doomed)
        .await
        .expect("workspace stays live");
    let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(tombstones, 0, "failure does not tombstone a live workspace");
    assert_eq!(count_history(&store, &keeper).await, 2 * 3 + PRESTAGED_ROWS);
    sqlx::query("DROP TRIGGER fail_message_cleanup")
        .execute(store.write_pool())
        .await
        .unwrap();
    store.delete_agent_session(&doomed, &removed).await.unwrap();
    store
        .delete_workspace(&doomed)
        .await
        .expect("retry succeeds");
    assert_deleted(&store, &doomed, 3).await;
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn workspace_delete_cancellation_can_retry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    seed_history(&store, &doomed, "interrupted", HISTORY_ROWS).await;
    let initial = count_history(&store, &doomed).await;
    let mut deleting = Box::pin(store.delete_workspace(&doomed));
    let observe = async {
        loop {
            let mut tx = store
                .write_pool()
                .begin()
                .await
                .expect("observer transaction");
            let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_message_payload")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            if remaining > 0 && remaining < HISTORY_ROWS + PRESTAGED_ROWS {
                // Hold the only writer while cancelling: no in-flight
                // SQLite statement can commit another batch behind the test.
                return tx;
            }
            tx.commit().await.unwrap();
        }
    };
    let held = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            held = observe => held,
            result = &mut deleting => panic!("deletion finished without observable progress: {result:?}"),
        }
    })
    .await
    .expect("observe partial deletion");
    drop(deleting);
    held.rollback().await.unwrap();
    let remaining = count_history(&store, &doomed).await;
    assert!(remaining > 0 && remaining < initial);
    store
        .get_workspace(&doomed)
        .await
        .expect("live after cancellation");
    store
        .delete_workspace(&doomed)
        .await
        .expect("retry succeeds");
    assert_deleted(&store, &doomed, 0).await;
}

async fn seed_tab_and_draft(store: &Store, workspace: &WorkspaceId, host: &ClientId) {
    store
        .upsert_client(host, None, None, &ClientHostInfo::default())
        .await
        .unwrap();
    store
        .upsert_draft(
            workspace,
            &AgentId::from("draft-agent"),
            host,
            "keep draft",
            None,
        )
        .await
        .unwrap();
    store
        .upsert_browser_tab(
            host,
            BrowserTabInput {
                tab_id: workspace.0.clone(),
                workspace_id: workspace.clone(),
                url: "https://example.test/".to_string(),
                requested_url: None,
                title: None,
                owner_agent_id: None,
                owner_agent_name: None,
                visibility: BrowserTabVisibility::Visible,
                emulated_size: None,
                displayed: Some(true),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn workspace_delete_final_failure_keeps_row_and_tombstone_atomic() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    seed_history(&store, &doomed, "doomed-agent", HISTORY_ROWS).await;
    let host = ClientId::from("host");
    seed_tab_and_draft(&store, &doomed, &host).await;
    seed_tab_and_draft(&store, &keeper, &host).await;
    sqlx::query(
        "CREATE TRIGGER fail_tombstone BEFORE INSERT ON deleted_workspace_id \
         BEGIN SELECT RAISE(ABORT, 'injected tombstone failure'); END",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::Internal(message)) if message.contains("injected tombstone failure")
    ));
    store
        .get_workspace(&doomed)
        .await
        .expect("root delete rolled back");
    assert_eq!(count_history(&store, &doomed).await, 0);
    assert_eq!(
        store.browser_tab_displayed.len(),
        1,
        "committed tab cleanup evicts only doomed overlays"
    );
    assert!(store.get_browser_tab(&doomed.0).await.unwrap().is_none());
    assert_eq!(
        store
            .get_browser_tab(&keeper.0)
            .await
            .unwrap()
            .unwrap()
            .displayed,
        Some(true)
    );
    assert!(store
        .get_draft(&doomed, &AgentId::from("draft-agent"), &host)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_draft(&keeper, &AgentId::from("draft-agent"), &host)
        .await
        .unwrap()
        .is_some());
    let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(tombstones, 0);
    sqlx::query("DROP TRIGGER fail_tombstone")
        .execute(store.write_pool())
        .await
        .unwrap();
    store
        .delete_workspace(&doomed)
        .await
        .expect("retry succeeds");
    assert_deleted(&store, &doomed, 0).await;

    // Draft workspace keys are opaque. NotFound must leave sentinel drafts
    // alone, including on a retry after a successful deletion.
    store
        .upsert_draft(
            &doomed,
            &AgentId::from("draft-agent"),
            &host,
            "sentinel",
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store
            .get_draft(&doomed, &AgentId::from("draft-agent"), &host)
            .await
            .unwrap()
            .unwrap()
            .text,
        "sentinel"
    );
}

async fn seed_heavy_workspace_children(store: &Store, workspace: &WorkspaceId, count: i64) {
    let mut tx = store.write_pool().begin().await.unwrap();
    // Many notes, plus a single note with many versions/comments and children:
    // batching note rows alone would still cascade this entire fan-out.
    for insert in [
        "INSERT INTO note (id, workspace_id, title, content, parent_id, created_at, updated_at) \
         SELECT 'note-' || n, ?2, 'Note', 'content', CASE WHEN n > 1 THEN 'note-1' END, 't0', 't0' FROM rows",
        "INSERT INTO note_version (workspace_id, note_id, v, date, author_id, author_name, author_type, title, content) \
         SELECT ?2, 'note-1', n, 't0', 'author', 'Author', 'user', 'Note', 'version content' FROM rows",
        "INSERT INTO comment (id, workspace_id, note_id, thread_id, kind, content, author, author_type, anchor_json, created_at, updated_at) \
         SELECT ?2 || '-comment-' || n, ?2, 'note-1', 'thread', 'comment', 'comment content', 'author', 'user', '{}', 't0', 't0' FROM rows",
        "INSERT INTO note_line_attribution (workspace_id, note_id, computed_at, attributions_json) \
         SELECT ?2, 'note-' || n, 't0', '[]' FROM rows",
        "INSERT INTO tracked_changes (id, workspace_id, path, stage, status, created_at, updated_at) \
         SELECT ?2 || '-change-' || n, ?2, 'file-' || n, 'committed', 'modified', 't0', 't0' FROM rows",
        "INSERT INTO diffs (id, workspace_id, file_path, old_content, new_content, created_at, updated_at) \
         SELECT ?2 || '-diff-' || n, ?2, 'file-' || n, 'before', 'after', 't0', 't0' FROM rows",
        "INSERT INTO workspace_context_item (workspace_id, id, ordinal, payload) \
         SELECT ?2, 'item-' || n, n, '{}' FROM rows",
    ] {
        sqlx::query(&format!(
            "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM rows WHERE n < ?1) {insert}"
        ))
        .bind(count)
        .bind(&workspace.0)
        .execute(&mut *tx)
        .await
        .expect("seed heavy workspace child");
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn workspace_delete_sweeps_heavy_children_before_final_transaction() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    seed_heavy_workspace_children(&store, &doomed, HISTORY_ROWS).await;
    seed_heavy_workspace_children(&store, &keeper, 3).await;
    // These guards run inside the REAL cascades, so they reject hiding any
    // of this growing data in either the note or workspace delete statement.
    sqlx::raw_sql(
        "CREATE TRIGGER guard_note_cascade BEFORE DELETE ON note WHEN OLD.workspace_id = 'doomed' \
           AND (EXISTS(SELECT 1 FROM note_version WHERE workspace_id = OLD.workspace_id AND note_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM comment WHERE workspace_id = OLD.workspace_id AND note_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM note WHERE workspace_id = OLD.workspace_id AND parent_id = OLD.id)) \
         BEGIN SELECT RAISE(ABORT, 'heavy note cascade'); END; \
         CREATE TRIGGER guard_workspace_cascade BEFORE DELETE ON workspace WHEN OLD.id = 'doomed' \
           AND (EXISTS(SELECT 1 FROM note WHERE workspace_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM tracked_changes WHERE workspace_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM diffs WHERE workspace_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM workspace_context_item WHERE workspace_id = OLD.id)) \
         BEGIN SELECT RAISE(ABORT, 'heavy workspace cascade'); END;",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    let finished = AtomicBool::new(false);
    let deleting = async {
        let result = store.delete_workspace(&doomed).await;
        finished.store(true, Ordering::SeqCst);
        result
    };
    let writing = async {
        let mut saw_partial = false;
        while !finished.load(Ordering::SeqCst) {
            let mut tx = store.write_pool().begin().await.unwrap();
            let remaining: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM note_version WHERE workspace_id = ?")
                    .bind(&doomed.0)
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            sqlx::query("UPDATE workspace SET title = 'write during note cleanup' WHERE id = ?")
                .bind(&keeper.0)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            saw_partial |= remaining > 0 && remaining < HISTORY_ROWS;
        }
        saw_partial
    };
    let (deleted, saw_partial) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(deleting, writing)
    })
    .await
    .expect("cleanup completes");
    deleted.expect("no heavy final cascade");
    assert!(
        saw_partial,
        "another writer commits between note version batches"
    );
    for table in [
        "note",
        "note_version",
        "note_line_attribution",
        "comment",
        "tracked_changes",
        "diffs",
        "workspace_context_item",
    ] {
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(total, 3, "{table} keeps only unrelated rows");
        let deleted: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE workspace_id = ?"
        ))
        .bind(&doomed.0)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
        assert_eq!(deleted, 0, "all scoped {table} rows deleted");
    }
    let kept_parents: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note WHERE workspace_id = ? AND parent_id IS NOT NULL",
    )
    .bind(&keeper.0)
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(kept_parents, 2, "unrelated note parent links untouched");
    assert_deleted(&store, &doomed, 0).await;
}

#[tokio::test]
async fn workspace_delete_sweeps_accumulated_agent_children() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    let agent = seed_history(&store, &doomed, "doomed-agent", HISTORY_ROWS).await;
    seed_history(&store, &keeper, "keeper-agent", 3).await;
    let mut tx = store.write_pool().begin().await.unwrap();
    for insert in [
        "INSERT INTO agent_queue (id, agent_id, position, payload, created_at) \
         SELECT 'queue-' || n, ?2, n, '{}', 't0' FROM rows",
        "INSERT INTO hook (hook_id, workspace_id, agent_id, name, code, delay_ms, state, created_at) \
         SELECT 'hook-' || n, 'doomed', ?2, 'old hook', 'return {}', 10000, 'dispatched', 't0' FROM rows",
        "INSERT INTO pr_monitor (monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number, state, created_at, updated_at) \
         SELECT 'monitor-' || n, 'doomed', ?2, 'owner', 'repo', n, 'completed', 't0', 't0' FROM rows",
    ] {
        sqlx::query(&format!(
            "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM rows WHERE n < ?1) {insert}"
        ))
        .bind(HISTORY_ROWS)
        .bind(&agent.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    for (parent, child) in [
        ("doomed-agent", "keeper-agent"),
        ("keeper-agent", "doomed-agent"),
        ("keeper-agent", "keeper-agent"),
    ] {
        sqlx::query("INSERT INTO completion_wake_delivery (parent_agent_id, child_agent_id, completion_identity, delivered_at) VALUES (?, ?, 'done', 't0')")
            .bind(parent).bind(child).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO advisory_wake_delivery (parent_agent_id, child_agent_id, delivered_at) VALUES (?, ?, 't0')")
            .bind(parent).bind(child).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    sqlx::query(
        "CREATE TRIGGER guard_session_cascade BEFORE DELETE ON agent_session WHEN OLD.id = 'doomed-agent' \
           AND (EXISTS(SELECT 1 FROM agent_message WHERE agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM agent_message_payload WHERE agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM agent_queue WHERE agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM hook WHERE agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM pr_monitor WHERE agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM completion_wake_delivery WHERE parent_agent_id = OLD.id OR child_agent_id = OLD.id) \
             OR EXISTS(SELECT 1 FROM advisory_wake_delivery WHERE parent_agent_id = OLD.id OR child_agent_id = OLD.id)) \
         BEGIN SELECT RAISE(ABORT, 'heavy session cascade'); END",
    )
    .execute(store.write_pool()).await.unwrap();
    assert!(
        !store.delete_agent_session(&keeper, &agent).await.unwrap(),
        "wrong workspace is a no-op"
    );
    store
        .delete_workspace(&doomed)
        .await
        .expect("no heavy session cascade");
    for table in ["agent_queue", "hook", "pr_monitor"] {
        let remaining: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(remaining, 0, "{table} cleaned up");
    }
    for table in ["completion_wake_delivery", "advisory_wake_delivery"] {
        let pairs: Vec<(String, String)> = sqlx::query_as(&format!(
            "SELECT parent_agent_id, child_agent_id FROM {table}"
        ))
        .fetch_all(store.read_pool())
        .await
        .unwrap();
        assert_eq!(
            pairs,
            vec![("keeper-agent".to_string(), "keeper-agent".to_string())]
        );
    }
    assert_deleted(&store, &doomed, 3).await;
}

/// Simulate a session created after the session sweep but before finalization.
/// This must be a retryable failure, never a fallback giant history cascade.
#[tokio::test]
async fn workspace_delete_rejects_session_created_after_sweep() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    seed_heavy_workspace_children(&store, &doomed, 1).await;
    sqlx::raw_sql(
        "CREATE TRIGGER race_session_after_sweep AFTER DELETE ON note \
         BEGIN \
           INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
           VALUES ('racing', OLD.workspace_id, 'racing session', 'idle', 't0', 't0'); \
           INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
           VALUES ('racing-message', 'racing', 0, 'assistant', '\"searchable\"', 't0'); \
         END;",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::Internal(message)) if message.contains("gained an agent during deletion")
    ));
    store
        .get_workspace(&doomed)
        .await
        .expect("workspace still live");
    assert_eq!(count_history(&store, &doomed).await, 1);
    store
        .delete_workspace(&doomed)
        .await
        .expect("retry sweeps new session");
    assert_deleted(&store, &doomed, 0).await;
}

#[tokio::test]
async fn workspace_delete_browser_batch_failure_evicts_only_committed_overlays() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let doomed = seed_workspace(&store, "doomed").await;
    let keeper = seed_workspace(&store, "keeper").await;
    let host = ClientId::from("host");
    seed_tab_and_draft(&store, &keeper, &host).await;
    for n in 0..501 {
        store
            .upsert_browser_tab(
                &host,
                BrowserTabInput {
                    tab_id: format!("doomed-{n}"),
                    workspace_id: doomed.clone(),
                    url: "https://example.test/".to_string(),
                    requested_url: None,
                    title: None,
                    owner_agent_id: None,
                    owner_agent_name: None,
                    visibility: BrowserTabVisibility::Visible,
                    emulated_size: None,
                    displayed: Some(true),
                },
            )
            .await
            .unwrap();
    }
    sqlx::query(
        "CREATE TRIGGER fail_browser_cleanup BEFORE DELETE ON browser_tab \
         WHEN OLD.workspace_id = 'doomed' AND \
              (SELECT COUNT(*) FROM browser_tab WHERE workspace_id = OLD.workspace_id) = 1 \
         BEGIN SELECT RAISE(ABORT, 'injected browser cleanup failure'); END",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(matches!(
        store.delete_workspace(&doomed).await,
        Err(Error::Internal(message)) if message.contains("injected browser cleanup failure")
    ));
    let remaining = store.list_browser_tabs(&doomed).await.unwrap();
    assert_eq!(remaining.len(), 1, "first 500 tab deletes stayed committed");
    assert_eq!(
        remaining[0].displayed,
        Some(true),
        "failed batch keeps its overlay"
    );
    assert_eq!(
        store.browser_tab_displayed.len(),
        2,
        "only remaining and unrelated overlays survive"
    );
    store
        .get_workspace(&doomed)
        .await
        .expect("live after failed batch");
    sqlx::query("DROP TRIGGER fail_browser_cleanup")
        .execute(store.write_pool())
        .await
        .unwrap();
    store
        .delete_workspace(&doomed)
        .await
        .expect("retry succeeds");
    assert_eq!(store.browser_tab_displayed.len(), 1);
    assert_eq!(
        store
            .get_browser_tab(&keeper.0)
            .await
            .unwrap()
            .unwrap()
            .displayed,
        Some(true)
    );
    assert_deleted(&store, &doomed, 0).await;
}
