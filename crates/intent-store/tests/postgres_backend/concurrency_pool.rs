//! The tenant writer lock against a shared pool: one tenant's writers park
//! at most one connection, lock waits are bounded and retried, a stuck
//! writer loses the lock, and the pool's timeouts surface as `StoreBusy`.
//!
//! "Another holder" of a tenant's writer lock is played by an admin
//! transaction that locks the tenant's `tenant_writer` row: what another
//! daemon, or another store handle for the tenant, looks like to this store.

use std::time::Duration;

use intent_core::Error;
use intent_store::postgres::{self, PgPool, PoolSettings, PostgresOptions, TenantId, WriterLimits};
use intent_store::Store;
use intentd_test_support::test_timeout;

use super::harness::{empty_db, hold_writer_lock, writer_lock_is_free, writer_lock_waiters};

/// A migrated database: its URL and an admin pool.
async fn migrated_db() -> Option<(String, PgPool)> {
    let (url, admin) = empty_db().await?;
    postgres::migrate(&admin).await.expect("migrate");
    Some((url, admin))
}

async fn open_on(pool: &PgPool, tenant: &str, writer: WriterLimits) -> Store {
    Store::open_postgres(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions {
            writer,
            ..PostgresOptions::default()
        },
    )
    .await
    .expect("open tenant store")
}

/// Limits that never run out during a test: only the lock holder decides.
/// One lock attempt spans the whole budget, so a waiting writer stays in the
/// lock (never between a timed-out attempt and its retry) while observed.
fn patient() -> WriterLimits {
    let budget = test_timeout(Duration::from_secs(60));
    WriterLimits {
        wait_budget: budget,
        lock_attempt: budget,
        ..WriterLimits::default()
    }
}

async fn release_writer_lock(held: sqlx::Transaction<'static, sqlx::Postgres>) {
    held.rollback().await.expect("release the writer lock");
}

/// Poll `check` until it holds; Postgres emits no event when a lock wait
/// starts or a session ends.
async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(test_timeout(Duration::from_secs(15)), async {
        while !check().await {
            // timing-guard: poll interval; Postgres emits no event for lock waits or ended sessions.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// Audit finding 2: tenant A's writers used to check out a connection each
/// and then wait in the writer lock, so a burst of them behind a held lock
/// took every pooled connection and tenant B's next checkout timed out.
#[tokio::test]
async fn a_saturated_tenant_cannot_starve_another_tenant_on_a_small_pool() {
    let Some((url, admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect_with(
        &url,
        PoolSettings {
            acquire_timeout: test_timeout(Duration::from_secs(5)),
            ..PoolSettings::new(4)
        },
    )
    .await
    .expect("connect the small pool");
    let a = open_on(&pool, "tenant-a", patient()).await;
    let b = open_on(&pool, "tenant-b", patient()).await;

    let held = hold_writer_lock(&admin, "tenant-a").await;
    let writers: Vec<_> = (0..8)
        .map(|i| {
            let a = a.clone();
            tokio::spawn(async move { a.set_setting(&format!("k{i}"), "\"a\"").await })
        })
        .collect();
    wait_until("all 8 of tenant A's writers queue", || {
        let a = a.clone();
        async move { a.postgres_writers_waiting() == 8 }
    })
    .await;
    wait_until("one of them waits in the lock", || {
        let admin = admin.clone();
        async move { !writer_lock_waiters(&admin).await.is_empty() }
    })
    .await;

    let b_work = async {
        b.set_setting("userRules", "\"b\"").await?;
        b.get_setting("userRules").await
    };
    let read = tokio::time::timeout(test_timeout(Duration::from_secs(30)), b_work)
        .await
        .expect("tenant B is not stuck behind tenant A")
        .expect("tenant B gets a connection while tenant A is saturated");
    assert_eq!(read.as_deref(), Some("\"b\""));
    assert_eq!(
        writer_lock_waiters(&admin).await.len(),
        1,
        "tenant A parks one connection in the lock, not one per writer"
    );
    assert_eq!(a.postgres_writers_waiting(), 8);

    release_writer_lock(held).await;
    for writer in writers {
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), writer)
            .await
            .expect("tenant A's writers finish once the lock is free")
            .unwrap()
            .expect("tenant A's write");
    }
    for i in 0..8 {
        assert_eq!(
            a.get_setting(&format!("k{i}")).await.unwrap().as_deref(),
            Some("\"a\"")
        );
    }
    assert_eq!(a.postgres_writers_waiting(), 0);
}

#[tokio::test]
async fn a_writer_that_cannot_get_the_lock_within_its_budget_is_busy_and_writes_nothing() {
    let Some((url, admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    let a = open_on(
        &pool,
        "tenant-a",
        WriterLimits {
            wait_budget: Duration::from_millis(300),
            lock_attempt: Duration::from_millis(100),
            ..WriterLimits::default()
        },
    )
    .await;

    let held = hold_writer_lock(&admin, "tenant-a").await;
    let busy = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        a.set_setting("userRules", "\"a\""),
    )
    .await
    .expect("the lock wait is bounded");
    assert!(matches!(busy, Err(Error::StoreBusy(_))), "{busy:?}");
    assert_eq!(a.postgres_writers_waiting(), 0);
    assert!(
        writer_lock_waiters(&admin).await.is_empty(),
        "a busy writer leaves no session waiting in the lock"
    );
    assert_eq!(a.get_setting("userRules").await.unwrap(), None);

    release_writer_lock(held).await;
    a.set_setting("userRules", "\"a\"").await.unwrap();
    assert_eq!(
        a.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
}

/// The retry policy: a lock attempt that times out rolls back and tries
/// again in a new transaction while the budget lasts.
#[tokio::test]
async fn a_writer_retries_the_lock_in_a_new_transaction_until_it_is_free() {
    let Some((url, admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    let a = open_on(
        &pool,
        "tenant-a",
        WriterLimits {
            lock_attempt: Duration::from_millis(100),
            ..patient()
        },
    )
    .await;

    let held = hold_writer_lock(&admin, "tenant-a").await;
    let writer = tokio::spawn({
        let a = a.clone();
        async move { a.set_setting("userRules", "\"a\"").await }
    });
    wait_until("the writer waits in the lock", || {
        let admin = admin.clone();
        async move { !writer_lock_waiters(&admin).await.is_empty() }
    })
    .await;
    let first = writer_lock_waiters(&admin).await.into_iter().next();
    let first = first.expect("the writer still waits in its first attempt");
    wait_until("the writer retries in a new transaction", || {
        let admin = admin.clone();
        let first = first.clone();
        async move {
            writer_lock_waiters(&admin)
                .await
                .iter()
                .any(|w| w.1 != first.1)
        }
    })
    .await;
    assert!(!writer.is_finished());

    release_writer_lock(held).await;
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), writer)
        .await
        .expect("the writer finishes once the lock is free")
        .unwrap()
        .expect("the retried write succeeds");
    assert_eq!(
        a.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
}

/// A writer whose client stops talking (here: a held owner-queue permit)
/// loses the tenant lock after `idle_in_transaction_timeout`, so other
/// daemons and store handles are not blocked for ever.
#[tokio::test]
async fn a_writer_left_idle_in_its_transaction_loses_the_tenant_lock() {
    let Some((url, admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    let a = open_on(
        &pool,
        "tenant-a",
        WriterLimits {
            idle_in_transaction_timeout: Duration::from_millis(300),
            ..patient()
        },
    )
    .await;
    let owner = a.get_primary_principal().await.unwrap().id;

    let permit = a
        .owner_queue_permit(&owner, None)
        .await
        .unwrap()
        .expect("the primary principal owns the queue");
    assert!(
        !writer_lock_is_free(&admin, "tenant-a").await,
        "the permit holds the tenant lock"
    );
    tokio::time::timeout(test_timeout(Duration::from_secs(15)), async {
        while !writer_lock_is_free(&admin, "tenant-a").await {
            // timing-guard: poll interval; Postgres emits no event when it ends an idle session.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the server ends the idle writer and frees the tenant lock");
    drop(permit);
}

#[tokio::test]
async fn a_checkout_past_the_acquire_timeout_is_busy() {
    let Some((url, _admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect_with(
        &url,
        PoolSettings {
            acquire_timeout: Duration::from_millis(200),
            ..PoolSettings::new(1)
        },
    )
    .await
    .expect("connect");
    assert_eq!(
        pool.options().get_acquire_timeout(),
        Duration::from_millis(200)
    );
    assert_eq!(pool.options().get_max_connections(), 1);
    let a = open_on(&pool, "tenant-a", patient()).await;
    let only = pool.acquire().await.expect("the only connection");
    let read = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        a.get_setting("userRules"),
    )
    .await
    .expect("the checkout is bounded");
    assert!(matches!(read, Err(Error::StoreBusy(_))), "{read:?}");
    drop(only);
    assert_eq!(a.get_setting("userRules").await.unwrap(), None);
}

/// [`postgres::lock_tenant_writer`] for tenant A: a 100 ms attempt,
/// one-minute statement and idle timeouts, then a 10 minute `lock_timeout`.
async fn run_writer_lock_sql(conn: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    let limits = WriterLimits {
        lock_attempt: Duration::from_secs(600),
        statement_timeout: Duration::from_secs(60),
        idle_in_transaction_timeout: Duration::from_secs(60),
        ..WriterLimits::default()
    };
    postgres::lock_tenant_writer(conn, "tenant-a", Duration::from_millis(100), &limits).await
}

/// The lock statement waits under the attempt's `lock_timeout` and leaves
/// the fixed one for the rest of the transaction.
#[tokio::test]
async fn the_lock_statement_waits_under_the_attempt_timeout_then_resets_it() {
    let Some((_url, admin)) = migrated_db().await else {
        return;
    };
    let held = hold_writer_lock(&admin, "tenant-a").await;
    let mut tx = admin.begin().await.unwrap();
    let waited = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        run_writer_lock_sql(&mut tx),
    )
    .await
    .expect("the wait uses the attempt's 100 ms, not the reset value");
    let code = waited
        .expect_err("the lock is held")
        .into_database_error()
        .and_then(|e| e.code().map(std::borrow::Cow::into_owned));
    assert_eq!(code.as_deref(), Some("55P03"));
    tx.rollback().await.unwrap();
    release_writer_lock(held).await;

    let mut tx = admin.begin().await.unwrap();
    run_writer_lock_sql(&mut tx)
        .await
        .expect("the free lock is granted");
    let settings: (String, String, String) = sqlx::query_as(
        "SELECT current_setting('lock_timeout'), current_setting('statement_timeout'), \
         current_setting('idle_in_transaction_session_timeout')",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(settings, ("10min".into(), "1min".into(), "1min".into()));
    tx.rollback().await.unwrap();
}

/// The writer's budget also bounds its checkout, when the pool's own
/// acquire timeout is longer: the only connection frees after the budget,
/// and the writer is busy instead of writing late.
#[tokio::test]
async fn a_writer_waiting_for_a_connection_is_busy_within_its_budget() {
    let Some((url, _admin)) = migrated_db().await else {
        return;
    };
    let pool = postgres::connect_with(
        &url,
        PoolSettings {
            acquire_timeout: test_timeout(Duration::from_secs(600)),
            ..PoolSettings::new(1)
        },
    )
    .await
    .expect("connect");
    let a = open_on(
        &pool,
        "tenant-a",
        WriterLimits {
            wait_budget: Duration::from_millis(300),
            ..WriterLimits::default()
        },
    )
    .await;
    let only = pool.acquire().await.expect("the only connection");
    let frees = tokio::spawn(async move {
        // timing-guard: the connection must free after the writer's 300 ms budget, not on an event.
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(only);
    });
    let busy = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        a.set_setting("userRules", "\"a\""),
    )
    .await
    .expect("the checkout is bounded by the writer's budget");
    assert!(matches!(busy, Err(Error::StoreBusy(_))), "{busy:?}");
    assert_eq!(a.postgres_writers_waiting(), 0);
    frees.await.unwrap();
    assert_eq!(a.get_setting("userRules").await.unwrap(), None);
}

/// A server that accepts the TCP connection and never answers.
#[tokio::test]
async fn connect_gives_up_after_its_connect_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let silent = tokio::spawn(async move {
        let mut open = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            open.push(socket);
        }
    });
    let url = format!("postgresql://nobody@127.0.0.1:{port}/db?sslmode=disable");
    let connecting = postgres::connect_with(
        &url,
        PoolSettings {
            connect_timeout: Duration::from_millis(200),
            acquire_timeout: test_timeout(Duration::from_secs(600)),
            ..PoolSettings::new(1)
        },
    );
    let failed = tokio::time::timeout(test_timeout(Duration::from_secs(30)), connecting)
        .await
        .expect("connect is bounded by its own timeout");
    let message = failed.expect_err("a silent server").to_string();
    assert!(message.contains("no connection after 200 ms"), "{message}");
    silent.abort();
}
