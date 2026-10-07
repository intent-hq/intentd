//! Test-only tracing plumbing shared by every capture-style subscriber test
//! in this crate (regression: monorepo#3580).
//!
//! Tests that assert on captured `tracing` events install a thread-local
//! subscriber via `tracing::subscriber::set_default`. Without a global
//! default, `tracing-core`'s callsite interest cache can be poisoned to
//! `never` under parallel tests: when its dispatcher registry believes there
//! is at most one live dispatcher, a callsite registering on another thread
//! rebuilds its interest *without the registry lock* from that thread's
//! current default — `NoSubscriber` on a thread with no local subscriber,
//! yielding `Interest::never()`. That unlocked store races the locked rebuild
//! triggered by a test's `set_default` and can land after it (lost update). A
//! cached `never` short-circuits `enabled()` entirely, so the capture sees
//! zero events and nothing rebuilds the cache before the test asserts.
//!
//! [`set_capture_default`] closes the race by installing a process-global
//! [`InterestAnchor`] (once) before setting the thread-local capture. The
//! anchor replaces the `NoSubscriber` fallback: every rebuild path now
//! resolves interest to at least `sometimes`, which forces the per-event
//! `enabled()` check instead of dropping events at the callsite.
//!
//! The anchor is also the crate's SQL statement counter
//! ([`count_sqlx_statements`]): sqlx-sqlite emits its per-statement
//! `sqlx::query` event from the connection's worker thread, inside the
//! caller's span (forwarded with the command), so a thread-local capture on
//! the test thread never sees it — only the process-global dispatcher does.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};

use tracing::Instrument as _;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Registry;

/// Install `capture` as the thread-local default, with the process-global
/// [`InterestAnchor`] in place first. Use this instead of calling
/// `tracing::subscriber::set_default` directly in any test that asserts on
/// captured events.
pub(crate) fn set_capture_default<S>(capture: S) -> tracing::subscriber::DefaultGuard
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    install_anchor();
    tracing::subscriber::set_default(capture)
}

/// Run `fut` and count the `sqlx::query` statement events sqlx emits while it
/// runs — one per executed statement, the same signal the daemon's
/// `rpc_profile` statement budget counts. Attribution is span-scoped: the
/// future is instrumented with a marker span, sqlx forwards that span to its
/// worker thread with every command, and only events under the marker are
/// counted, so concurrent tests in one process never inflate each other. Use
/// [`warm_sqlx_pool`] first to exclude lazy connection setup from a query-cost
/// budget. One uncounted request is insufficient: `SQLx` returns connections
/// asynchronously and a later acquire can open another connection, counting
/// its setup PRAGMA batch inside the acquiring span.
///
/// The calling thread must NOT hold a thread-local capture (the marker span
/// would then be created by the capture while the worker-thread events reach
/// the anchor, and the two never meet).
pub(crate) async fn count_sqlx_statements<F: Future>(fut: F) -> (F::Output, usize) {
    install_anchor();
    let span = tracing::trace_span!(STATEMENT_COUNT_SPAN);
    let id = span
        .id()
        .expect("the anchor enables the statement-count marker span")
        .into_u64();
    let counter = Arc::new(AtomicUsize::new(0));
    counters().lock().unwrap().insert(id, counter.clone());
    // `span` outlives the instrumented clone so the registry cannot recycle
    // its id before the counter entry is gone.
    let out = fut.instrument(span.clone()).await;
    let statements = counter.load(Ordering::SeqCst);
    counters().lock().unwrap().remove(&id);
    drop(span);
    (out, statements)
}

/// Executed SQL and hydrated row count, scoped like `count_sqlx_statements`.
#[derive(Debug, Default)]
pub(crate) struct SqlxQuery {
    pub sql: String,
    pub rows_returned: u64,
}

impl tracing::field::Visit for SqlxQuery {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "db.statement" {
            self.sql = value.to_owned();
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "rows_returned" {
            self.rows_returned = value;
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

type QueryCapture = Arc<Mutex<Vec<SqlxQuery>>>;

fn query_captures() -> &'static Mutex<HashMap<u64, QueryCapture>> {
    static CAPTURES: OnceLock<Mutex<HashMap<u64, QueryCapture>>> = OnceLock::new();
    CAPTURES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Capture real worker-thread statements, including their returned-row counts.
/// Warm the pool first and do not hold a thread-local tracing capture.
pub(crate) async fn capture_sqlx_queries<F: Future>(fut: F) -> (F::Output, Vec<SqlxQuery>) {
    install_anchor();
    let span = tracing::trace_span!(STATEMENT_COUNT_SPAN);
    let id = span.id().expect("query capture span").into_u64();
    let capture = Arc::new(Mutex::new(Vec::new()));
    query_captures().lock().unwrap().insert(id, capture.clone());
    let out = fut.instrument(span.clone()).await;
    query_captures().lock().unwrap().remove(&id);
    let queries = std::mem::take(&mut *capture.lock().unwrap());
    drop(span);
    (out, queries)
}

fn install_anchor() {
    static ANCHOR: Once = Once::new();
    ANCHOR.call_once(|| {
        tracing::subscriber::set_global_default(Registry::default().with(InterestAnchor)).expect(
            "intent-services tests own this process's global tracing default; \
             a competing set_global_default call would silently re-expose the \
             monorepo#3580 interest-cache race",
        );
    });
}

/// Target of sqlx's per-statement event (see `sqlx_core::logger`).
const SQLX_QUERY_TARGET: &str = "sqlx::query";
/// Name of the marker span [`count_sqlx_statements`] scopes its count to.
const STATEMENT_COUNT_SPAN: &str = "intent_services_sqlx_statement_count";

/// Live [`count_sqlx_statements`] counters keyed by marker span id.
fn counters() -> &'static Mutex<HashMap<u64, Arc<AtomicUsize>>> {
    static COUNTERS: OnceLock<Mutex<HashMap<u64, Arc<AtomicUsize>>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Process-global fallback layer (over a [`Registry`], which supplies the
/// span store and `current_span` that sqlx's worker-thread forwarding needs)
/// that pins every callsite's interest at `sometimes` so it can never be
/// cached as `never` (see the module docs). It consumes nothing except the
/// statement-count plumbing: `enabled()` is `true` only for the
/// [`STATEMENT_COUNT_SPAN`] marker span and the [`SQLX_QUERY_TARGET`]
/// statement event, so threads without a thread-local capture drop every
/// other event exactly as they did with no global default.
struct InterestAnchor;

impl<S> Layer<S> for InterestAnchor
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        // The target match deliberately ignores the callsite kind: sqlx
        // pre-checks with `tracing::enabled!`, whose callsite is a bare
        // `Kind::HINT` (not an event), and skips the emit when that says no.
        (metadata.is_span() && metadata.name() == STATEMENT_COUNT_SPAN)
            || metadata.target() == SQLX_QUERY_TARGET
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != SQLX_QUERY_TARGET {
            return;
        }
        let Some(marker) = ctx
            .event_span(event)
            .into_iter()
            .flat_map(|span| span.scope())
            .find(|span| span.name() == STATEMENT_COUNT_SPAN)
        else {
            return;
        };
        if let Some(counter) = counters().lock().unwrap().get(&marker.id().into_u64()) {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(capture) = query_captures()
            .lock()
            .unwrap()
            .get(&marker.id().into_u64())
        {
            let mut query = SqlxQuery::default();
            event.record(&mut query);
            capture.lock().unwrap().push(query);
        }
    }
}

/// Initialize every pool slot before measuring an exact query-cost budget.
/// Hold all connections at once so acquisitions cannot reuse a warmed slot
/// while leaving another lazy. Dropping them may return them asynchronously,
/// but the pool is already at capacity: subsequent reads wait for these
/// initialized connections instead of opening new ones.
///
/// Call outside the counted span, with no connections checked out. This is
/// for short tests that do not close/expire connections during measurement;
/// it does not filter any SQL events or alter production pool behavior.
pub(crate) async fn warm_sqlx_pool(pool: &intent_store::StorePool) {
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await.expect("warm SQL statement-count pool"));
    }
}

#[intent_test_macros::daemon_test]
async fn warmed_pool_statement_count_survives_connection_contention() {
    // Force a connection beyond the one a single warm-up read can touch.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_lazy("sqlite::memory:")
        .unwrap();
    let pool = intent_store::StorePool::from(pool);
    warm_sqlx_pool(&pool).await;
    let mut held = Vec::new();
    for _ in 1..pool.options().get_max_connections() {
        held.push(pool.acquire().await.unwrap());
    }
    let ((), count) = count_sqlx_statements(async {
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("SELECT 1")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("SELECT 2")
            .execute(&mut *connection)
            .await
            .unwrap();
    })
    .await;
    assert_eq!(count, 2, "only the two application statements are counted");
    drop(held);
    pool.close().await;
}

#[intent_test_macros::daemon_test]
async fn warmed_pool_counts_extra_queries_and_isolates_concurrent_spans() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_lazy("sqlite::memory:")
        .unwrap();
    let pool = intent_store::StorePool::from(pool);
    warm_sqlx_pool(&pool).await;
    let barrier = tokio::sync::Barrier::new(2);
    let read = async |statements| {
        let mut connection = pool.acquire().await.unwrap();
        // Both measured spans are live and own distinct connections before
        // either issues SQL; a process-global counter would conflate them.
        barrier.wait().await;
        for _ in 0..statements {
            sqlx::query("SELECT 1")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
    };
    let (((), one), ((), extra)) = tokio::join!(
        count_sqlx_statements(read(1)),
        count_sqlx_statements(read(2)),
    );
    assert_eq!(one, 1);
    assert_eq!(extra, 2, "a real extra application query must still count");
    pool.close().await;
}
