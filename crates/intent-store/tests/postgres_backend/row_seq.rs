//! `row_seq` is a tenant's insertion order on Postgres.
//!
//! On sharded Neki each router hands out identity values in batches of 1000,
//! so with more than one router an identity column numbers rows 1, 1001, 2,
//! 1002, ... and every `ORDER BY row_seq` returns them out of insertion order.
//! These tests move a table's identity sequence between inserts the way a
//! second router would, then check the store still returns insertion order.

use intent_core::{Workspace, WorkspaceId};
use intent_store::postgres::PgPool;
use intent_store::Store;

use crate::harness::{admin_write, exec, hold_writer_lock, open, test_db};

const T0: &str = "2026-01-01T00:00:00.000Z";

async fn insert_workspace(store: &Store, id: &str) {
    store
        .insert_workspace(&Workspace {
            id: WorkspaceId(id.into()),
            title: id.into(),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

/// Makes `table`'s identity hand out `next` on its next insert, as a router
/// with a different batch would. A no-op once `row_seq` has no identity.
async fn router_batch(admin: &PgPool, table: &str, next: i64) {
    exec(
        admin,
        format!(
            "DO $$ BEGIN \
               IF pg_get_serial_sequence('{table}', 'row_seq') IS NOT NULL THEN \
                 PERFORM setval(pg_get_serial_sequence('{table}', 'row_seq'), {next}, false); \
               END IF; \
             END $$"
        ),
    )
    .await;
}

async fn row_seq(admin: &PgPool, table: &str, tenant: &str, id: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT row_seq FROM {table} WHERE tenant_id = $1 AND id = $2"
    )))
    .bind(tenant)
    .bind(id)
    .fetch_one(admin)
    .await
    .unwrap()
}

#[tokio::test]
async fn row_seq_follows_insertion_order_across_router_batches() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "row-seq-order";
    let store = open(&db, tenant).await;

    insert_workspace(&store, "w1").await;
    let first = row_seq(&db.admin, "workspace", tenant, "w1").await;
    // Router B's batch starts 1000 above router A's.
    router_batch(&db.admin, "workspace", first + 1000).await;
    insert_workspace(&store, "w2").await;
    // Back on router A: its batch still has values below router B's.
    router_batch(&db.admin, "workspace", first + 1).await;
    insert_workspace(&store, "w3").await;

    let ids: Vec<String> = store
        .ordinary_workspace_ids()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.0)
        .collect();
    assert_eq!(
        ids,
        ["w1", "w2", "w3"],
        "ORDER BY row_seq is insertion order"
    );
    let seqs = [
        first,
        row_seq(&db.admin, "workspace", tenant, "w2").await,
        row_seq(&db.admin, "workspace", tenant, "w3").await,
    ];
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "row_seq {seqs:?}");
}

#[tokio::test]
async fn tenants_number_rows_independently() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "row-seq-a").await;
    let b = open(&db, "row-seq-b").await;
    for i in 0..3 {
        insert_workspace(&a, &format!("a{i}")).await;
        insert_workspace(&b, &format!("b{i}")).await;
    }
    for (store, prefix) in [(&a, "a"), (&b, "b")] {
        let ids: Vec<String> = store
            .ordinary_workspace_ids()
            .await
            .unwrap()
            .into_iter()
            .map(|w| w.0)
            .collect();
        let want: Vec<String> = (0..3).map(|i| format!("{prefix}{i}")).collect();
        assert_eq!(ids, want);
    }
}

/// A tenant whose rows predate the counter (`last_row_seq` NULL) starts
/// numbering above its largest existing `row_seq`, so new rows sort after
/// the old ones.
#[tokio::test]
async fn a_tenant_counter_starts_above_its_existing_rows() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "row-seq-floor";
    let store = open(&db, tenant).await;
    insert_workspace(&store, "old").await;
    admin_write(
        &db.admin,
        tenant,
        format!(
            "INSERT INTO known_repo (tenant_id, path, name, owner, added_at, last_used_at, row_seq) \
             VALUES ('{tenant}', '/r/old', 'old', NULL, '{T0}', '{T0}', 50000); \
             UPDATE tenant_writer SET last_row_seq = NULL WHERE tenant_id = '{tenant}'"
        ),
    )
    .await;
    insert_workspace(&store, "new").await;
    let new = row_seq(&db.admin, "workspace", tenant, "new").await;
    assert!(new > 50_000, "new row_seq {new} must sort after 50000");
    let ids: Vec<String> = store
        .ordinary_workspace_ids()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.0)
        .collect();
    assert_eq!(ids, ["old", "new"]);
}

/// Every table with a `row_seq` column takes it from the tenant counter:
/// no identity is left to hand out values, and the trigger fills it.
#[tokio::test]
async fn every_row_seq_table_uses_the_tenant_counter() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let identity: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.columns \
         WHERE table_schema = current_schema() AND column_name = 'row_seq' \
           AND is_identity = 'YES' ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert!(
        identity.is_empty(),
        "row_seq still an identity on {identity:?}"
    );
    let untriggered: Vec<String> = sqlx::query_scalar(
        "SELECT c.table_name::text FROM information_schema.columns c \
         WHERE c.table_schema = current_schema() AND c.column_name = 'row_seq' \
           AND NOT EXISTS (SELECT 1 FROM pg_trigger t \
             JOIN pg_class r ON r.oid = t.tgrelid \
             JOIN pg_namespace n ON n.oid = r.relnamespace \
             WHERE n.nspname = c.table_schema AND r.relname = c.table_name \
               AND t.tgname = 'row_seq_from_tenant_counter' AND NOT t.tgisinternal) \
         ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert!(
        untriggered.is_empty(),
        "row_seq tables without the tenant-counter trigger: {untriggered:?}"
    );
}

fn known_repo_insert(tenant: &str, path: &str) -> String {
    format!(
        "INSERT INTO known_repo (tenant_id, path, name, owner, added_at, last_used_at) \
         VALUES ('{tenant}', '{path}', 'r', NULL, '{T0}', '{T0}')"
    )
}

/// The SQLSTATE and message of `sql` run in `tx`, which must fail.
async fn failure(tx: &mut sqlx::PgConnection, sql: String) -> (String, String) {
    let err = sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(tx)
        .await
        .expect_err("the insert must fail");
    let sqlx::Error::Database(e) = err else {
        panic!("not a database error: {err}");
    };
    (
        e.code().unwrap_or_default().into_owned(),
        e.message().to_owned(),
    )
}

/// An insert that does not hold the row's tenant writer lock fails loudly
/// instead of taking an unordered (or NULL) `row_seq`: with no lock, with
/// another tenant's lock, and with an explicit `row_seq`.
#[tokio::test]
async fn an_insert_without_its_tenant_writer_lock_fails() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let _a = open(&db, "row-seq-lock-a").await;
    let _b = open(&db, "row-seq-lock-b").await;

    let mut tx = db.admin.begin().await.unwrap();
    let (code, message) = failure(&mut tx, known_repo_insert("row-seq-lock-a", "/r/1")).await;
    assert_eq!(code, "55000", "{message}");
    assert!(
        message.contains("without its tenant_writer lock"),
        "{message}"
    );
    tx.rollback().await.unwrap();

    let mut tx = hold_writer_lock(&db.admin, "row-seq-lock-b").await;
    let (code, message) = failure(&mut tx, known_repo_insert("row-seq-lock-a", "/r/2")).await;
    assert_eq!(code, "55000", "another tenant's lock: {message}");
    tx.rollback().await.unwrap();

    let mut tx = db.admin.begin().await.unwrap();
    let (code, _) = failure(
        &mut tx,
        format!(
            "INSERT INTO known_repo (tenant_id, path, name, owner, added_at, last_used_at, row_seq) \
             VALUES ('row-seq-lock-a', '/r/3', 'r', NULL, '{T0}', '{T0}', 7)"
        ),
    )
    .await;
    assert_eq!(code, "55000", "an explicit row_seq still needs the lock");
    tx.rollback().await.unwrap();
}

/// With no `tenant_writer` row there is no counter: the insert fails rather
/// than storing a NULL or 0 `row_seq`.
#[tokio::test]
async fn an_insert_without_a_tenant_writer_row_fails() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "row-seq-no-counter";
    let mut tx = db.admin.begin().await.unwrap();
    // Pretend to hold a lock on a row that does not exist.
    sqlx::query("SELECT set_config('intent.writer_tenant', $1, true)")
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .unwrap();
    let (code, message) = failure(&mut tx, known_repo_insert(tenant, "/r/1")).await;
    assert_eq!(code, "55000", "{message}");
    assert!(message.contains("no tenant_writer row"), "{message}");
    tx.rollback().await.unwrap();
}

/// One statement inserting several rows numbers them in order, each above
/// the rows before it.
#[tokio::test]
async fn a_multi_row_insert_numbers_its_rows_in_order() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "row-seq-multi";
    let _store = open(&db, tenant).await;
    admin_write(&db.admin, tenant, known_repo_insert(tenant, "/r/0")).await;
    admin_write(
        &db.admin,
        tenant,
        format!(
            "INSERT INTO known_repo (tenant_id, path, name, owner, added_at, last_used_at) \
             SELECT '{tenant}', '/r/' || i, 'r', NULL, '{T0}', '{T0}' FROM generate_series(1, 5) AS i \
             ORDER BY i"
        ),
    )
    .await;
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT path, row_seq FROM known_repo WHERE tenant_id = $1 ORDER BY row_seq",
    )
    .bind(tenant)
    .fetch_all(&db.admin)
    .await
    .unwrap();
    let paths: Vec<&str> = rows.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(paths, ["/r/0", "/r/1", "/r/2", "/r/3", "/r/4", "/r/5"]);
    assert!(rows.windows(2).all(|w| w[0].1 < w[1].1), "{rows:?}");
}
