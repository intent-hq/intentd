//! The multi-tenant Postgres backend against a real database.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (a role that
//! may create databases and roles). Without the URL the tests skip, unless
//! `INTENT_STORE_REQUIRE_PG=1` (set in CI) turns the skip into a failure.
//!
//! Each test creates a fresh database and applies the migrations. Isolation
//! tests open two tenants that use identical ids, so a statement missing its
//! tenant filter returns or changes the other tenant's rows and fails. One
//! test installs forced row-level security policies on every tenant table
//! and connects as a non-superuser (superusers bypass them) to prove the
//! store's `rls_guard` mode works with them, for a later production opt-in.
#![cfg(feature = "postgres")]

#[path = "postgres_backend/harness.rs"]
mod harness;
#[path = "postgres_backend/settings.rs"]
mod settings;
#[path = "postgres_backend/workspaces.rs"]
mod workspaces;

use harness::{open, open_with, test_db};
use intent_store::postgres::TenantId;

#[tokio::test]
async fn settings_are_isolated_between_tenants_with_identical_keys() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;

    a.set_setting("userRules", "\"a\"").await.unwrap();
    b.set_setting("userRules", "\"b\"").await.unwrap();
    assert_eq!(
        a.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );

    a.set_setting("userRules", "\"a2\"").await.unwrap();
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );

    assert!(a.delete_setting("userRules").await.unwrap());
    assert_eq!(a.get_setting("userRules").await.unwrap(), None);
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );
    assert!(!a.delete_setting("userRules").await.unwrap());
}

#[tokio::test]
async fn each_tenant_has_its_own_stable_daemon_id() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    let a_id = a.daemon_id().await.unwrap();
    assert_eq!(a.daemon_id().await.unwrap(), a_id);
    assert_ne!(b.daemon_id().await.unwrap(), a_id);
    let reopened = open(&db, "tenant-a").await;
    assert_eq!(reopened.daemon_id().await.unwrap(), a_id);
}

#[tokio::test]
async fn opening_a_tenant_provisions_its_seed_rows_once() {
    let Some(db) = test_db(false).await else {
        return;
    };
    for _ in 0..2 {
        open(&db, "tenant-a").await;
        open(&db, "tenant-b").await;
    }
    let primaries: Vec<(String, i64)> = sqlx::query_as(
        "SELECT tenant_id, count(*) FROM principal WHERE is_primary = 1 GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert_eq!(primaries, [("tenant-a".into(), 1), ("tenant-b".into(), 1)]);
    let chiefs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workspace WHERE id = '__chief__' AND owner_principal_id IS NOT NULL",
    )
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!(chiefs, 2);
}

#[tokio::test]
async fn row_level_security_policies_work_with_the_guard_mode() {
    let Some(db) = test_db(true).await else {
        return;
    };
    let a = open_with(&db, "tenant-a", true).await;
    let b = open_with(&db, "tenant-b", true).await;
    a.set_setting("k", "1").await.unwrap();
    b.set_setting("k", "2").await.unwrap();

    let mut tx = db.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('intent.tenant_id', 'tenant-a', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let seen: Vec<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key = 'k'")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    assert_eq!(seen, ["1"]);
    let foreign =
        sqlx::query("INSERT INTO settings (tenant_id, key, value) VALUES ('tenant-b', 'x', '1')")
            .execute(&mut *tx)
            .await;
    assert!(
        foreign.is_err(),
        "the guard must reject a write for another tenant"
    );
    drop(tx);

    let unset: i64 = sqlx::query_scalar("SELECT count(*) FROM settings")
        .fetch_one(&db.app)
        .await
        .unwrap();
    assert_eq!(unset, 0, "without a tenant the guard shows no rows");
}

#[tokio::test]
async fn an_unported_method_reports_an_error_instead_of_touching_sqlite() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let err = a
        .delete_workspace(&intent_core::WorkspaceId("ws-1".into()))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("delete_workspace is not yet supported on the Postgres store"),
        "{err}"
    );
}

#[tokio::test]
async fn closing_one_tenant_store_leaves_the_shared_pool_open() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    a.close().await;
    b.set_setting("k", "1")
        .await
        .expect("tenant B still works after A closes");
    assert_eq!(b.get_setting("k").await.unwrap().as_deref(), Some("1"));
}

#[test]
fn tenant_ids_are_validated() {
    assert!(TenantId::new("org_1:prod-eu.a").is_ok());
    for bad in ["", "has space", "semi;colon", "quote'", &"x".repeat(129)] {
        assert!(TenantId::new(bad).is_err(), "{bad:?} must be rejected");
    }
}
