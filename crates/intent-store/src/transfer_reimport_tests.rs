//! Deleted workspace metadata must not prevent a safe, atomic re-import.

use crate::Store;
use intent_core::{AgentId, WorkspaceId};
use serde_json::{json, Value};

type Rows = Vec<(String, Vec<Value>)>;

fn rows(ws: &str) -> Rows {
    vec![
        (
            "workspace".into(),
            vec![
                json!({"id":ws, "title":"Imported", "branch":"main", "status":"Active", "created_at":"t0", "updated_at":"t0"}),
            ],
        ),
        (
            "agent_session".into(),
            vec![
                json!({"id":format!("agent-{ws}"), "workspace_id":ws, "name":"Agent", "status":"idle", "created_at":"t0", "updated_at":"t0"}),
            ],
        ),
        (
            "interrupted_agent".into(),
            vec![
                json!({"agent_id":format!("agent-{ws}"), "workspace_id":ws, "prev_status":"active", "interrupted_at":"t0", "resolution":"resumed"}),
            ],
        ),
        (
            "script".into(),
            vec![
                json!({"id":format!("script-{ws}"), "workspace_id":ws, "name":"Script", "command":"true", "mode":"command", "source":"user", "created_at":"t0"}),
            ],
        ),
        (
            "attachments".into(),
            vec![
                json!({"id":format!("attachment-{ws}"), "workspace_id":ws, "file_name":"file.txt", "size":3, "uploaded_at":"t0", "stored_path":".intent/attachments/file.txt"}),
            ],
        ),
    ]
}

async fn key(store: &Store, ws: &str, attachment: &str) {
    sqlx::query("INSERT INTO attachment_idempotency_keys (workspace_id, key, attachment_id, fingerprint, created_at) VALUES (?, 'retry', ?, 'fingerprint', 't0')")
        .bind(ws).bind(attachment).execute(store.write_pool()).await.unwrap();
}

async fn keys(store: &Store) -> Vec<(String, String, String, String, String)> {
    sqlx::query_as("SELECT workspace_id, key, attachment_id, fingerprint, created_at FROM attachment_idempotency_keys ORDER BY workspace_id, key")
        .fetch_all(store.read_pool()).await.unwrap()
}

async fn exported(store: &Store, ws: &str) -> Rows {
    store
        .transfer_export_rows(&WorkspaceId::from(ws))
        .await
        .unwrap()
}

async fn assert_foreign_keys(store: &Store) {
    let enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(store.write_pool())
        .await
        .unwrap();
    assert_eq!(enabled, 1);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(store.read_pool())
        .await
        .unwrap()
        .is_empty());
}

// Raw deletion reproduces metadata left by historical daemons, independently
// of the fixed public delete path. All FKs remain enabled.
async fn orphan(store: &Store, ws: &str) {
    store.transfer_import_rows(&rows(ws)).await.unwrap();
    key(store, ws, &format!("attachment-{ws}")).await;
    for sql in [
        "UPDATE interrupted_agent SET interrupted_at='old' WHERE workspace_id=?",
        "UPDATE script SET command='old command' WHERE workspace_id=?",
        "UPDATE attachments SET file_name='old.txt' WHERE workspace_id=?",
    ] {
        sqlx::query(sql)
            .bind(ws)
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM workspace WHERE id = ?")
        .bind(ws)
        .execute(store.write_pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn reimport_reclaims_matching_orphans_from_each_metadata_table() {
    let mut failures = Vec::new();
    for conflict in ["interrupted_agent", "script", "attachments"] {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("db")).await.unwrap();
        orphan(&store, "deleted").await;
        // Keep only this collision so each failure reports its own constraint.
        for table in [
            "interrupted_agent",
            "script",
            "attachment_idempotency_keys",
            "attachments",
        ] {
            if table != conflict
                && !(conflict == "attachments" && table == "attachment_idempotency_keys")
            {
                sqlx::query(&format!("DELETE FROM {table}"))
                    .execute(store.write_pool())
                    .await
                    .unwrap();
            }
        }
        orphan(&store, "other-orphan").await;
        if conflict == "attachments" {
            sqlx::query("WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<1201) INSERT INTO attachment_idempotency_keys (workspace_id, key, attachment_id, fingerprint, created_at) SELECT 'deleted', 'retry-' || n, 'attachment-deleted', 'fp', 't0' FROM rows")
                .execute(store.write_pool()).await.unwrap();
        }
        let unrelated = exported(&store, "other-orphan").await;
        let incoming = rows("deleted");
        let result = store.transfer_import_rows(&incoming).await;
        if let Err(err) = &result {
            failures.push(format!("{conflict}: {err}"));
            continue;
        }
        assert_eq!(result.unwrap(), 5);
        let actual = exported(&store, "deleted").await;
        for (table, incoming_rows) in &incoming {
            let actual_row = &actual.iter().find(|(t, _)| t == table).unwrap().1[0];
            for (column, value) in incoming_rows[0].as_object().unwrap() {
                assert_eq!(
                    &actual_row[column], value,
                    "incoming {table}.{column} wins over orphan history"
                );
            }
        }
        assert_eq!(exported(&store, "other-orphan").await, unrelated);
        assert_eq!(
            keys(&store).await.len(),
            1,
            "only the unrelated retry key remains"
        );
        assert_eq!(keys(&store).await[0].0, "other-orphan");
        assert_foreign_keys(&store).await;
    }
    assert!(failures.is_empty(), "orphan collisions: {failures:#?}");
}

#[tokio::test]
async fn reimport_cleanup_rolls_back_with_a_later_insert_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    orphan(&store, "deleted").await;
    let before = exported(&store, "deleted").await;
    let before_keys = keys(&store).await;
    let mut incoming = rows("deleted");
    incoming
        .last_mut()
        .unwrap()
        .1
        .push(json!({"id":"bad", "workspace_id":"deleted"}));
    let err = store.transfer_import_rows(&incoming).await.unwrap_err();
    assert!(
        err.to_string().contains("attachments.file_name"),
        "failure must occur after orphan cleanup: {err}"
    );
    assert_eq!(exported(&store, "deleted").await, before);
    assert_eq!(keys(&store).await, before_keys);
    assert_foreign_keys(&store).await;
}

#[tokio::test]
async fn reimport_preserves_live_workspaces_and_foreign_global_ids() {
    for collision in [
        "workspace",
        "agent_session",
        "interrupted_agent",
        "script",
        "attachments",
        "foreign_attachment_key",
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("db")).await.unwrap();
        orphan(&store, "deleted").await;
        store.transfer_import_rows(&rows("keeper")).await.unwrap();
        key(&store, "keeper", "attachment-keeper").await;
        match collision {
            "workspace" => {
                store
                    .transfer_import_rows(&rows("deleted")[..1])
                    .await
                    .unwrap();
            }
            "agent_session" => {
                sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) VALUES ('agent-deleted', 'keeper', 'Unrelated', 'idle', 't0', 't0')")
                    .execute(store.write_pool()).await.unwrap();
            }
            "foreign_attachment_key" => {
                sqlx::query("INSERT INTO attachment_idempotency_keys (workspace_id, key, attachment_id, fingerprint, created_at) VALUES ('keeper', 'foreign', 'attachment-deleted', 'foreign', 't0')")
                    .execute(store.write_pool()).await.unwrap();
            }
            table => {
                sqlx::query(&format!(
                    "UPDATE {table} SET workspace_id = 'keeper' WHERE workspace_id = 'deleted'"
                ))
                .execute(store.write_pool())
                .await
                .unwrap();
            }
        }
        let before = exported(&store, "deleted").await;
        let keeper = exported(&store, "keeper").await;
        let before_keys = keys(&store).await;
        let mut incoming = rows("deleted");
        if collision == "agent_session" {
            // Also protect a globally live agent when the archive contains
            // only its recovery row, not a colliding session insert.
            incoming.retain(|(t, _)| t != "agent_session");
        }
        assert!(
            store.transfer_import_rows(&incoming).await.is_err(),
            "must reject {collision}"
        );
        assert_eq!(exported(&store, "deleted").await, before, "{collision}");
        assert_eq!(exported(&store, "keeper").await, keeper, "{collision}");
        assert_eq!(keys(&store).await, before_keys, "{collision}");
        assert_foreign_keys(&store).await;
    }
}

#[tokio::test]
async fn reimport_does_not_sweep_orphans_absent_from_the_archive() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    orphan(&store, "deleted").await;
    let before = exported(&store, "deleted").await;
    let before_keys = keys(&store).await;
    // Metadata-only calls have no authority to reclaim absent workspaces.
    assert!(store
        .transfer_import_rows(&rows("deleted")[2..])
        .await
        .is_err());
    assert_eq!(exported(&store, "deleted").await, before);
    assert_eq!(keys(&store).await, before_keys);
    assert!(store
        .delete_workspace(&WorkspaceId::from("deleted"))
        .await
        .is_err());
    assert_eq!(exported(&store, "deleted").await, before);
    assert_eq!(keys(&store).await, before_keys);
    store
        .transfer_import_rows(&rows("deleted")[..1])
        .await
        .unwrap();
    let after = exported(&store, "deleted").await;
    for (table, old_rows) in before {
        if table != "workspace" {
            assert_eq!(
                after.iter().find(|(t, _)| t == &table).unwrap().1,
                old_rows,
                "{table}"
            );
        }
    }
    assert_eq!(keys(&store).await, before_keys);
}

#[tokio::test]
async fn delete_workspace_cleans_metadata_and_allows_reimport() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    store.transfer_import_rows(&rows("deleted")).await.unwrap();
    key(&store, "deleted", "attachment-deleted").await;
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    key(&store, "keeper", "attachment-keeper").await;
    let keeper = exported(&store, "keeper").await;
    let archive = exported(&store, "deleted").await;
    let ws = WorkspaceId::from("deleted");
    store.delete_workspace(&ws).await.unwrap();
    for (table, entries) in exported(&store, "deleted").await {
        assert!(entries.is_empty(), "deletion left {table}: {entries:?}");
    }
    assert_eq!(keys(&store).await.len(), 1);
    assert!(store.workspace_id_ever_used(&ws).await.unwrap());
    store.transfer_import_rows(&archive).await.unwrap();
    assert_eq!(exported(&store, "deleted").await, archive);
    assert_eq!(exported(&store, "keeper").await, keeper);
    assert_foreign_keys(&store).await;
}

#[tokio::test]
async fn delete_agent_cleans_only_its_owned_recovery_record() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    store.transfer_import_rows(&rows("deleted")).await.unwrap();
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    let before = exported(&store, "deleted").await;
    let agent = AgentId::from("agent-deleted");
    assert!(!store
        .delete_agent_session(&WorkspaceId::from("keeper"), &agent)
        .await
        .unwrap());
    assert_eq!(exported(&store, "deleted").await, before);
    assert!(store
        .delete_agent_session(&WorkspaceId::from("deleted"), &agent)
        .await
        .unwrap());
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM interrupted_agent WHERE agent_id = 'agent-deleted'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(count, 0, "individual deletion must remove recovery history");
    assert_eq!(
        exported(&store, "keeper")
            .await
            .iter()
            .find(|(t, _)| t == "interrupted_agent")
            .unwrap()
            .1
            .len(),
        1
    );
    sqlx::query(
        "UPDATE interrupted_agent SET workspace_id='deleted' WHERE agent_id='agent-keeper'",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    assert!(store
        .delete_agent_session(&WorkspaceId::from("keeper"), &AgentId::from("agent-keeper"))
        .await
        .unwrap());
    let preserved: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM interrupted_agent WHERE agent_id='agent-keeper' AND workspace_id='deleted'")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(
        preserved, 1,
        "agent deletion must match the recovery row's workspace"
    );
}

#[tokio::test]
async fn delete_workspace_preserves_retry_keys_bound_to_foreign_attachments() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    store.transfer_import_rows(&rows("deleted")).await.unwrap();
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    key(&store, "deleted", "attachment-keeper").await;
    let keeper = exported(&store, "keeper").await;
    let before_keys = keys(&store).await;
    store
        .delete_workspace(&WorkspaceId::from("deleted"))
        .await
        .unwrap();
    assert_eq!(keys(&store).await, before_keys);
    assert_eq!(exported(&store, "keeper").await, keeper);
    assert_foreign_keys(&store).await;
}

#[tokio::test]
async fn delete_workspace_preserves_foreign_attachment_dependencies_and_agent_history() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    store.transfer_import_rows(&rows("deleted")).await.unwrap();
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    // Inconsistent workspace metadata must never authorize deleting another
    // workspace's attachment retry key or a live agent's recovery history.
    sqlx::query(
        "UPDATE interrupted_agent SET workspace_id = 'deleted' WHERE agent_id = 'agent-keeper'",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    key(&store, "keeper", "attachment-deleted").await;
    let before_keys = keys(&store).await;
    let ws = WorkspaceId::from("deleted");
    let err = store
        .delete_workspace(&ws)
        .await
        .expect_err("foreign FK blocks deletion");
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
    assert_eq!(keys(&store).await, before_keys);
    store
        .get_workspace(&ws)
        .await
        .expect("failed cleanup retains workspace");
    let owner: String = sqlx::query_scalar(
        "SELECT workspace_id FROM interrupted_agent WHERE agent_id = 'agent-keeper'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(
        owner, "deleted",
        "unrelated live agent's history remains untouched"
    );
    assert_foreign_keys(&store).await;
}

#[tokio::test]
async fn delete_workspace_drains_orphan_metadata_in_bounded_batches() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    store
        .transfer_import_rows(&rows("deleted")[..1])
        .await
        .unwrap();
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    let mut tx = store.write_pool().begin().await.unwrap();
    for insert in [
        "INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at) SELECT 'old-' || n, 'deleted', 'active', 't0' FROM rows",
        "INSERT INTO script (id, workspace_id, name, command, mode, source, created_at) SELECT 'script-' || n, 'deleted', 'S', 'true', 'command', 'user', 't0' FROM rows",
        "INSERT INTO attachments (id, workspace_id, file_name, size, uploaded_at, stored_path) SELECT 'attachment-' || n, 'deleted', 'f', 1, 't0', '.intent/attachments/f' FROM rows",
        "INSERT INTO attachment_idempotency_keys (workspace_id, key, attachment_id, fingerprint, created_at) SELECT 'deleted', 'key-' || n, 'attachment-' || n, 'fp', 't0' FROM rows",
    ] {
        sqlx::query(&format!("WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n < 1201) {insert}"))
            .execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    let finished = AtomicBool::new(false);
    let deletion = async {
        let result = store.delete_workspace(&WorkspaceId::from("deleted")).await;
        finished.store(true, Ordering::SeqCst);
        result.unwrap();
    };
    let observer = async {
        let mut previous = [1201_i64; 4];
        let mut partial = [false; 4];
        loop {
            // Taking the same fair writer pool interleaves with each cleanup
            // batch and measures committed row counts, without timing guesses.
            let mut tx = store.write_pool().begin().await.unwrap();
            for (i, table) in [
                "interrupted_agent",
                "script",
                "attachments",
                "attachment_idempotency_keys",
            ]
            .iter()
            .enumerate()
            {
                let remaining: i64 = sqlx::query_scalar(&format!(
                    "SELECT COUNT(*) FROM {table} WHERE workspace_id='deleted'"
                ))
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                assert!(previous[i] - remaining <= 500, "unbounded {table} cleanup");
                partial[i] |= remaining > 0 && remaining < 1201;
                previous[i] = remaining;
            }
            sqlx::query("UPDATE workspace SET title='Unrelated writer' WHERE id='keeper'")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            if finished.load(Ordering::SeqCst) {
                assert_eq!(previous, [0; 4]);
                assert_eq!(partial, [true; 4]);
                break;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::join!(deletion, observer)
    })
    .await
    .unwrap();
    assert_foreign_keys(&store).await;
}

// Mix live sessions, historical orphans and both directions of malformed
// recovery ownership. Only 1,201 of these rows belong to the deletion.
async fn seed_workspace_recovery(store: &Store) {
    store.transfer_import_rows(&rows("deleted")).await.unwrap();
    store.transfer_import_rows(&rows("keeper")).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
         VALUES ('foreign-history', 'deleted', 'A', 'idle', 't0', 't0'); \
         INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at) \
         VALUES ('foreign-history', 'keeper', 'active', 't0'); \
         UPDATE interrupted_agent SET workspace_id='deleted' WHERE agent_id='agent-keeper'; \
         WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<1200) \
         INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at) \
         SELECT 'orphan-' || n, 'deleted', 'active', 't0' FROM rows;",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
}

async fn remaining_workspace_recovery(store: &Store) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM interrupted_agent \
         WHERE workspace_id='deleted' AND agent_id!='agent-keeper'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap()
}

async fn assert_recovery_retry(store: &Store) {
    store
        .get_workspace(&WorkspaceId::from("deleted"))
        .await
        .expect("incomplete deletion retains workspace");
    let tombstones: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM deleted_workspace_id WHERE id='deleted'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(tombstones, 0);
    store
        .delete_workspace(&WorkspaceId::from("deleted"))
        .await
        .expect("retry drains the remainder");
    let preserved: Vec<(String, String)> =
        sqlx::query_as("SELECT agent_id, workspace_id FROM interrupted_agent ORDER BY agent_id")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(
        preserved,
        vec![
            ("agent-keeper".into(), "deleted".into()),
            ("foreign-history".into(), "keeper".into()),
        ],
        "neither direction of foreign recovery ownership authorizes removal"
    );
    assert!(store
        .get_agent_session(&AgentId::from("agent-keeper"))
        .await
        .is_ok());
    assert_foreign_keys(store).await;
}

#[tokio::test]
async fn workspace_recovery_failure_precedes_session_deletion_and_can_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    seed_workspace_recovery(&store).await;
    sqlx::query(
        "CREATE TRIGGER fail_recovery BEFORE DELETE ON interrupted_agent \
         WHEN OLD.workspace_id='deleted' AND \
              (SELECT COUNT(*) FROM interrupted_agent \
               WHERE workspace_id='deleted' AND agent_id!='agent-keeper') <= 701 \
         BEGIN SELECT RAISE(ABORT, 'injected recovery failure'); END",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    let err = store
        .delete_workspace(&WorkspaceId::from("deleted"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("injected recovery failure"),
        "{err}"
    );
    let sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE workspace_id='deleted'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(
        sessions, 2,
        "recovery must finish before deleting any session"
    );
    assert_eq!(remaining_workspace_recovery(&store).await, 701);
    sqlx::query("DROP TRIGGER fail_recovery")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_recovery_retry(&store).await;
}

#[tokio::test]
async fn workspace_recovery_stays_clean_after_session_failure_and_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    seed_workspace_recovery(&store).await;
    sqlx::query(
        "CREATE TRIGGER fail_session BEFORE DELETE ON agent_session \
         WHEN OLD.workspace_id='deleted' AND \
              (SELECT COUNT(*) FROM agent_session WHERE workspace_id='deleted') = 1 \
         BEGIN SELECT RAISE(ABORT, 'injected session failure'); END",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    let err = store
        .delete_workspace(&WorkspaceId::from("deleted"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("injected session failure"),
        "{err}"
    );
    assert_eq!(remaining_workspace_recovery(&store).await, 0);
    sqlx::query("DROP TRIGGER fail_session")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_recovery_retry(&store).await;
}

#[tokio::test]
async fn workspace_recovery_cancellation_keeps_sessions_and_can_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    seed_workspace_recovery(&store).await;
    let workspace = WorkspaceId::from("deleted");
    let mut deleting = Box::pin(store.delete_workspace(&workspace));
    let observe = async {
        loop {
            let mut tx = store.write_pool().begin().await.unwrap();
            let remaining: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM interrupted_agent \
                 WHERE workspace_id='deleted' AND agent_id!='agent-keeper'",
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();
            if remaining > 0 && remaining < 1201 {
                // Hold the sole writer until cancellation, so no later batch
                // can commit behind the assertions (no timing assumptions).
                return tx;
            }
            tx.commit().await.unwrap();
        }
    };
    let held = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::select! {
            held = observe => held,
            result = &mut deleting => panic!("deletion finished before cancellation: {result:?}"),
        }
    })
    .await
    .unwrap();
    drop(deleting);
    held.rollback().await.unwrap();
    assert_eq!(remaining_workspace_recovery(&store).await, 701);
    let sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE workspace_id='deleted'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(sessions, 2);
    assert_recovery_retry(&store).await;
}

#[tokio::test]
async fn attachment_deletion_uses_indexed_foreign_key_probes() {
    use sqlx::Row;
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).await.unwrap();
    let details: Vec<String> =
        sqlx::query("EXPLAIN QUERY PLAN DELETE FROM attachments WHERE id = ?")
            .bind("attachment")
            .fetch_all(store.read_pool())
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get("detail"))
            .collect();
    assert!(
        details
            .iter()
            .any(|d| d.contains("attachment_idempotency_keys") && d.starts_with("SEARCH")),
        "FK probe must use an attachment-id index: {details:?}"
    );
    assert!(
        !details
            .iter()
            .any(|d| d.starts_with("SCAN attachment_idempotency_keys")),
        "{details:?}"
    );
}
