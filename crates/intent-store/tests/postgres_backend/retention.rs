//! The retention sweeps a tenant runtime runs on Postgres: the idempotency
//! key reaper, the usage-rate reaper and the event retention sweep.
//!
//! Each way the sweep could go wrong has a test here:
//! - deleting another tenant's rows: the decoy tenant owns the same keys,
//!   old and recent, on the leak-trapped database, and is never touched;
//! - deleting recent rows: rows at or after the cutoff stay;
//! - an unbounded delete: rows go in batches, each its own writer
//!   transaction, and another writer of the tenant commits between two;
//! - deleting without the tenant writer lock: while another session holds
//!   it, the sweep waits and deletes nothing;
//! - running after a fence: a fenced store's sweep fails `NotOwner` and
//!   deletes nothing more, also when the fence fires between two batches.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use intent_store::postgres::delete_probe::{self, DeleteProbe};
use intent_store::postgres::{PgPool, PostgresOptions, TenantId, WriterLimits};
use intent_store::{Error, Store};
use intentd_test_support::test_timeout;
use tokio::sync::Notify;

use crate::harness::{
    admin_write, hold_writer_lock, open_trapped, tenant_rows, test_db, trapped_db,
    writer_lock_waiters, TestDb,
};

const OLD: &str = "2026-01-01T00:00:00.000Z";
const RECENT: &str = "2026-09-01T00:00:00.000Z";
const CUTOFF: &str = "2026-06-01T00:00:00.000Z";
/// Expired rows per table; with [`BATCH`] each sweep takes 3 batches.
const ROWS: i64 = 120;
const RECENT_ROWS: i64 = 5;
const BATCH: i64 = 50;

/// The sweeps under test, by the table each empties.
#[derive(Clone, Copy, Debug)]
enum Sweep {
    Idempotency,
    UsageRate,
    Events,
}

const SWEEPS: [Sweep; 3] = [Sweep::Idempotency, Sweep::UsageRate, Sweep::Events];

impl Sweep {
    fn table(self) -> &'static str {
        match self {
            Self::Idempotency => "idempotency_key",
            Self::UsageRate => "usage_rate_minutely",
            Self::Events => "event",
        }
    }

    /// The column the sweep's cutoff compares.
    fn time_column(self) -> &'static str {
        match self {
            Self::Idempotency => "created_at",
            Self::UsageRate => "bucket_utc",
            Self::Events => "timestamp",
        }
    }

    async fn run(self, store: &Store) -> intent_store::Result<u64> {
        match self {
            Self::Idempotency => store.reap_idempotent(CUTOFF).await,
            Self::UsageRate => store.delete_usage_rate_before(CUTOFF).await,
            Self::Events => store.delete_ephemeral_events_before(CUTOFF).await,
        }
    }
}

/// [`ROWS`] expired and [`RECENT_ROWS`] recent rows of every swept table,
/// plus old events the event sweep keeps (lifecycle, not ephemeral). Keys
/// are the same for every tenant, so the decoy shares them.
async fn seed(db: &TestDb, tenant: &str) {
    let mut sql = String::new();
    for (prefix, at, n) in [("old", OLD, ROWS), ("new", RECENT, RECENT_ROWS)] {
        write!(
            sql,
            "INSERT INTO idempotency_key (tenant_id, workspace_id, idempotency_key, method, \
             result_json, created_at) \
             SELECT '{tenant}', '', '{prefix}-' || g, 'm', '1', '{at}' \
             FROM generate_series(1, {n}) g; \
             INSERT INTO usage_rate_minutely (tenant_id, bucket_utc, input_tokens) \
             SELECT '{tenant}', '{at}' || '-' || lpad(g::text, 4, '0'), g \
             FROM generate_series(1, {n}) g; \
             INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor) \
             SELECT '{tenant}', '{prefix}-ev-' || g, 'ws-r', '{at}', 'agent:stream:delta', '{{}}' \
             FROM generate_series(1, {n}) g; "
        )
        .expect("write to String");
    }
    write!(
        sql,
        "INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor) \
         SELECT '{tenant}', 'keep-ev-' || g, 'ws-r', '{OLD}', 'workspace:created', '{{}}' \
         FROM generate_series(1, 3) g;"
    )
    .expect("write to String");
    admin_write(&db.admin, tenant, sql).await;
}

/// Rows of `tenant` in `table`, as (expired, recent).
async fn rows(db: &TestDb, tenant: &str, sweep: Sweep) -> (i64, i64) {
    let (table, column) = (sweep.table(), sweep.time_column());
    let filter = if matches!(sweep, Sweep::Events) {
        " AND event_type = 'agent:stream:delta'"
    } else {
        ""
    };
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FILTER (WHERE {column} COLLATE \"C\" < $2), \
                count(*) FILTER (WHERE {column} COLLATE \"C\" >= $2) \
         FROM {table} WHERE tenant_id = $1{filter}"
    )))
    .bind(tenant)
    .bind(CUTOFF)
    .fetch_one(&db.admin)
    .await
    .expect("count rows")
}

async fn kept_events(db: &TestDb, tenant: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM event WHERE tenant_id = $1 AND event_type = 'workspace:created'",
    )
    .bind(tenant)
    .fetch_one(&db.admin)
    .await
    .expect("count kept events")
}

/// A trapped database with the decoy tenant seeded, and its rows.
async fn with_decoy() -> Option<(TestDb, Vec<(String, String)>)> {
    let db = trapped_db().await?;
    seed(&db, "decoy").await;
    let before = tenant_rows(&db.admin, "decoy").await;
    Some((db, before))
}

async fn assert_decoy_untouched(db: &TestDb, before: &[(String, String)]) {
    assert!(
        tenant_rows(&db.admin, "decoy").await == before,
        "the subject's sweep changed the decoy tenant's rows"
    );
}

/// Install a batch-size probe for `tenant`. Probes are process-wide by
/// tenant name, so every test here uses its own subject tenants.
fn probe(tenant: &str, configure: impl FnOnce(&mut DeleteProbe)) -> Arc<DeleteProbe> {
    let mut probe = DeleteProbe {
        batch_rows: Some(BATCH),
        ..DeleteProbe::default()
    };
    configure(&mut probe);
    let probe = Arc::new(probe);
    delete_probe::set(tenant, Some(probe.clone()));
    probe
}

fn pausing_probe(tenant: &str) -> (Arc<DeleteProbe>, Arc<Notify>, Arc<Notify>) {
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let probe = probe(tenant, |p| {
        p.pause_after_first_batch = std::sync::Mutex::new(Some((entered.clone(), release.clone())));
    });
    (probe, entered, release)
}

async fn until_paused(entered: &Notify) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), entered.notified())
        .await
        .expect("the sweep ran a first batch");
}

fn batches_on(probe: &DeleteProbe, table: &str) -> Vec<u64> {
    probe
        .batches()
        .into_iter()
        .filter(|(t, _)| t == table)
        .map(|(_, rows)| rows)
        .collect()
}

#[tokio::test]
async fn each_sweep_removes_only_its_tenants_expired_rows_in_bounded_batches() {
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    for sweep in SWEEPS {
        let tenant = format!("rt-batches-{}", sweep.table().replace('_', "-"));
        let store = open_trapped(&db, &tenant).await;
        seed(&db, &tenant).await;
        let probe = probe(&tenant, |_| {});

        let removed = sweep.run(&store).await.unwrap();
        delete_probe::set(&tenant, None);

        assert_eq!(
            removed,
            ROWS.unsigned_abs(),
            "{sweep:?} removed every expired row"
        );
        assert_eq!(
            rows(&db, &tenant, sweep).await,
            (0, RECENT_ROWS),
            "{sweep:?}"
        );
        let batches = batches_on(&probe, sweep.table());
        assert_eq!(
            batches,
            [50, 50, 20],
            "{sweep:?} deleted in bounded batches"
        );
        assert_eq!(
            sweep.run(&store).await.unwrap(),
            0,
            "{sweep:?} is idempotent"
        );
    }
    assert_eq!(
        kept_events(&db, "rt-batches-event").await,
        3,
        "lifecycle events stay"
    );
    assert_decoy_untouched(&db, &decoy).await;
}

/// One `DELETE` of every expired row would hold the writer lock for all of
/// them, so a writer of the tenant would wait for the whole sweep; now it
/// runs between two batches.
#[tokio::test]
async fn a_writer_of_the_same_tenant_runs_between_sweep_batches() {
    let Some((db, decoy)) = with_decoy().await else {
        return;
    };
    for sweep in SWEEPS {
        let tenant = format!("rt-interleave-{}", sweep.table().replace('_', "-"));
        let store = open_trapped(&db, &tenant).await;
        seed(&db, &tenant).await;
        let (probe, entered, release) = pausing_probe(&tenant);

        let sweeping = {
            let store = store.clone();
            tokio::spawn(async move { sweep.run(&store).await })
        };
        until_paused(&entered).await;
        tokio::time::timeout(
            test_timeout(Duration::from_secs(5)),
            store.set_setting("userRules", "\"x\""),
        )
        .await
        .expect("a writer ran while the sweep was between batches")
        .unwrap();
        assert_eq!(batches_on(&probe, sweep.table()).len(), 1, "{sweep:?}");
        release.notify_one();

        let removed = sweeping.await.unwrap().unwrap();
        delete_probe::set(&tenant, None);
        assert_eq!(removed, ROWS.unsigned_abs(), "{sweep:?}");
        assert!(batches_on(&probe, sweep.table()).len() > 1, "{sweep:?}");
    }
    assert_decoy_untouched(&db, &decoy).await;
}

/// The sweep deletes under the tenant writer lock: while another session
/// (another daemon or store handle) holds it, the sweep waits for it and
/// deletes nothing, then finishes once it is released.
#[tokio::test]
async fn each_sweep_waits_for_the_tenant_writer_lock() {
    let Some(db) = test_db(false).await else {
        return;
    };
    for sweep in SWEEPS {
        let tenant = format!("rt-locked-{}", sweep.table().replace('_', "-"));
        let store = open_patient(&db.app, &tenant).await;
        seed(&db, &tenant).await;

        let held = hold_writer_lock(&db.admin, &tenant).await;
        let sweeping = {
            let store = store.clone();
            tokio::spawn(async move { sweep.run(&store).await })
        };
        wait_for_lock_waiter(&db.admin).await;
        assert_eq!(
            rows(&db, &tenant, sweep).await,
            (ROWS, RECENT_ROWS),
            "{sweep:?} deleted nothing without the writer lock"
        );
        assert!(!sweeping.is_finished(), "{sweep:?} waits for the lock");
        held.rollback().await.unwrap();

        let removed = tokio::time::timeout(test_timeout(Duration::from_secs(30)), sweeping)
            .await
            .expect("the sweep finishes once the lock is free")
            .unwrap()
            .unwrap();
        assert_eq!(removed, ROWS.unsigned_abs(), "{sweep:?}");
    }
}

/// A fenced tenant writes nothing: the sweep fails `NotOwner` before its
/// first batch, and a fence that fires between two batches stops the rest.
#[tokio::test]
async fn a_fenced_tenant_sweeps_nothing_more() {
    let Some(db) = test_db(false).await else {
        return;
    };
    for sweep in SWEEPS {
        let name = sweep.table().replace('_', "-");

        let tenant = format!("rt-fenced-{name}");
        let store = open_owned(&db.app, &tenant).await;
        seed(&db, &tenant).await;
        assert!(store.tenant_fence().expect("an owned store").fire("test"));
        let refused = sweep.run(&store).await;
        assert!(
            matches!(refused, Err(Error::NotOwner(_))),
            "{sweep:?}: {refused:?}"
        );
        assert_eq!(
            rows(&db, &tenant, sweep).await,
            (ROWS, RECENT_ROWS),
            "{sweep:?}"
        );

        let tenant = format!("rt-fenced-mid-{name}");
        let store = open_owned(&db.app, &tenant).await;
        seed(&db, &tenant).await;
        let (_probe, entered, release) = pausing_probe(&tenant);
        let sweeping = {
            let store = store.clone();
            tokio::spawn(async move { sweep.run(&store).await })
        };
        until_paused(&entered).await;
        assert!(store.tenant_fence().expect("an owned store").fire("test"));
        release.notify_one();
        let refused = sweeping.await.unwrap();
        delete_probe::set(&tenant, None);
        assert!(
            matches!(refused, Err(Error::NotOwner(_))),
            "{sweep:?}: {refused:?}"
        );
        assert_eq!(
            rows(&db, &tenant, sweep).await,
            (ROWS - BATCH, RECENT_ROWS),
            "{sweep:?}: only the batch before the fence was deleted"
        );
    }
}

/// A store whose writers wait for the lock as long as the test does.
async fn open_patient(pool: &PgPool, tenant: &str) -> Store {
    Store::open_postgres(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        patient(),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open tenant store")
}

/// A store that claimed `tenant`, so it has a fence.
async fn open_owned(pool: &PgPool, tenant: &str) -> Store {
    let (store, _lease) = Store::open_postgres_owned_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        patient(),
        "proc-a",
        "proc-a",
        Duration::from_secs(600),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant");
    store
}

fn patient() -> PostgresOptions {
    let long = test_timeout(Duration::from_secs(60));
    PostgresOptions {
        writer: WriterLimits {
            wait_budget: long,
            lock_attempt: long,
            ..WriterLimits::default()
        },
        ..PostgresOptions::default()
    }
}

async fn wait_for_lock_waiter(admin: &PgPool) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while writer_lock_waiters(admin).await.is_empty() {
            // timing-guard: poll pg_stat_activity until the sweep queues on the lock
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the sweep waits for the writer lock");
}
